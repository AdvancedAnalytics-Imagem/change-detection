# -*- encoding: utf-8 -*-
#!/usr/bin/python
import datetime
import os
import time
from datetime import date, datetime, timedelta
from zipfile import ZipFile

from arcpy import Exists
from core.libs.ProgressTracking import ProgressTracker
from core._constants import *
from sentinelsat.exceptions import ServerError as SetinelServerError

from .ErrorManager import FolderAccessError, MaxFailuresError


def load_path_and_name(wrapped):

    def wrapper(*args, **kwargs):
        if wrapped.__annotations__.get('path') and wrapped.__annotations__.get('name'):
            if not kwargs.get('name'):
                path = kwargs.get('path')
                if path and path != 'IN_MEMORY':
                    kwargs['name'] = os.path.basename(path)
                    kwargs['path'] = os.path.dirname(path)
            elif not kwargs.get('path'):
                name = kwargs.get('name')
                if name:
                    if name == 'IN_MEMORY':
                        kwargs['name'] = ''
                        kwargs['path'] = name
                    else:
                        kwargs['name'] = os.path.basename(name)
                        kwargs['path'] = os.path.dirname(name)
        return wrapped(*args, **kwargs)
    
    return wrapper

def prevent_server_error(wrapped_function):
    def reattempt_execution(*args, **kwargs):
        failed_attempts = 0
        while True:
            try:
                return wrapped_function(*args, **kwargs)
            except Exception as e:
                if not isinstance(e, SetinelServerError):
                    raise(e)
                if failed_attempts > 20:
                    raise MaxFailuresError(wrapped_function.__name__, attempts=failed_attempts)
                print(f'Sentinel Server error:\n{e}\nReattempting connection in a few...')
                failed_attempts += 1
                time.sleep(failed_attempts*20)

    return reattempt_execution


class BaseConfig:
    debug = True
    batch_size = 4000
    regular_sleep_time_seconds = 5
    progress_tracker: ProgressTracker = ProgressTracker()
    
    def format_date_as_str(self, date: datetime, format: str = "%Y-%m-%dT%H:%M:%S"):
        """Formats a datetime object on the format 1995/10/13T00:00:00"""
        return datetime.strftime(date, format)

    @property
    def now(self):
        return datetime.now()
    
    @property
    def now_str(self):
        return self.format_date_as_str(self.now)

    @property
    def today(self):
        return date.today()
    
    @property
    def today_str(self):
        return self.format_date_as_str(self.today, format='%Y%m%d')

    @load_path_and_name
    def unzip_file(self, path: str, name: str) -> str:
        full_path = os.path.join(path, name)

        if not name.endswith(".zip") or not os.path.exists(full_path):
            return
        
        try:
            zipObj = ZipFile(full_path, "r")
            zipObj.extractall(path)
            zipObj.close()
            os.remove(full_path)
        except Exception:
            print(f"O arquivo {name} está corrompido")
            os.remove(full_path)
        return full_path.replace('.zip','')

    def unzip_files(self, files: list = [], folder: str = []) -> list:
        extracted_list = []

        if not files and folder:
            if os.listdir(folder):
                # Extrai o arquivo zip e depois deleta o arquivo zip
                for item in os.listdir(folder):
                    extracted_list.append(self.unzip_file(path=folder, name=item))
            else:
                print(f"Pasta {folder} vazia")
        elif files:
            for item in files:
                extracted_list.append(self.unzip_file(path=folder, name=item))

        return [item for item in extracted_list if item]

class BasePath:
    path:str = ''
    name: str = ''
    full_path: str = ''

    @load_path_and_name
    def __init__(self, path: str = None, name: str = None, *args, **kwargs):
            
        if path and name:
            name = name.replace(' ','_').replace(':','')
            self.name = name
            self.load_base_path_variables(path=path)
    
    def load_base_path_variables(self, path: str):
        self.load_path_variable(path)
        self.full_path = os.path.join(self.path, self.name)

    def load_path_variable(self, path: str, subsequent_folders: list = []) -> str:
        """Loads a path variable and guarantees it exists and is accessible

            Args:
                path (str): Folder path string
                subsequent_folders (list, optional): Next folders to be acessed. Defaults to [].

            Raises:
                FolderAccessError, 
                FolderAccessError

            Returns:
                str: Compiled folder path string
        """
        if not os.path.exists(path) and not Exists(path):
            try:
                os.makedirs(path)
            except Exception as e:
                raise FolderAccessError(folder=path)

        if subsequent_folders and not isinstance(subsequent_folders, list):
            subsequent_folders = [subsequent_folders]

        for subsequent_folder in subsequent_folders:
            path = os.path.join(path, subsequent_folder)
            if not os.path.exists(path) and not Exists(path):
                try:
                    os.makedirs(path)
                except Exception as e:
                    raise FolderAccessError(folder=path)

        self.path = path
        return path

    @property
    def exists(self):
        return Exists(self.full_path)
        # raise UnexistingFeatureError(feature=self.full_path)
        
    def get_files_by_extension(self, folder: str, extension: str = '.jp2', limit: int = 0) -> list:
        """List filed full path based on the desired extension
            Args:
                folder (str): Base folder to search for files
                extension (str, optional): File extension to be look after. Defaults to '.jp2'.
                limit (int, optional): File name size limitation. Defaults to 0.
            Returns:
                list: List of encontered files
        """        
        encountered_files = []
        if not os.path.exists(folder): return []
        for path, dirs, files in os.walk(folder):
            for file in files:
                if file.endswith(extension):
                    file_path = os.path.join(path, file)
                    if limit and len(file) > limit:
                        file_path = self.rename_files(file=file_path, limit=limit)
                    encountered_files.append(file_path)
        return encountered_files

    def rename_files(self, file: str, limit: int = 0):
        file_name = os.path.basename(file)
        if limit and len(file_name) > limit:
            path = os.path.dirname(file)
            name = file_name.split(".")[0][-limit:]
            extension = file_name.split('.')[-1]
            reduced_name = f'{name}.{extension}'

            new_name = self.get_unique_name(path=path, name=reduced_name)
            new_full_path = os.path.join(path, reduced_name)
            os.rename(file, new_full_path)
            return new_full_path

    @staticmethod
    @load_path_and_name
    def get_unique_name(name: str, path: str) -> str:
        name_without_extension = os.path.splitext(name)[0]
        extension = os.path.splitext(name)[-1]
        feature_name = f'{name_without_extension}{extension}'
        for i in range(1, 20):
            if not Exists(os.path.join(path, feature_name)):
                return feature_name
            feature_name = f'{name_without_extension}_{i}{extension}'
        return name
