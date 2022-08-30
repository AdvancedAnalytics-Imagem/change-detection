# -*- encoding: utf-8 -*-
#!/usr/bin/python
import os
import time
from datetime import date, datetime
from zipfile import ZipFile

from arcpy import Exists
from arcpy.management import Delete
from core._constants import *
from core._logs import *
from core.libs.ProgressTracking import ProgressTracker
from sentinelsat.exceptions import ServerError as SetinelServerError

from .ErrorManager import (FolderAccessError, InvalidPathError,
                           MaxFailuresError, UnexistingFeatureError)


def load_path_and_name(wrapped):
    def wrapper(*args, **kwargs):
        if wrapped.__annotations__.get('path') and wrapped.__annotations__.get('name'):
            path = kwargs.get('path')
            if not isinstance(path, str) and hasattr(path, 'full_path'):
                kwargs['path'] = path.full_path
            
            name = kwargs.get('name')
            if not name and path and path != 'IN_MEMORY':
                kwargs['name'] = os.path.basename(path)
                kwargs['path'] = os.path.dirname(path)
            elif not path and name and name != 'IN_MEMORY':
                kwargs['name'] = os.path.basename(name)
                kwargs['path'] = os.path.dirname(name)
        return wrapped(*args, **kwargs)
    return wrapper

def delete_source_files(wrapped):
    def wrapper(self, *args, **kwargs):
        source = None
        if self.full_path and Exists(self.full_path):
            source = self.full_path
        response = wrapped(self, *args, **kwargs)

        if source and Exists(source) and self.delete_temp_files_while_processing:
            if source != response:
                Delete(source)
        return response
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
                if failed_attempts > MaxFailuresError.max_failures:
                    raise MaxFailuresError(wrapped_function.__name__, attempts=failed_attempts)
                aprint(f'Sentinel Server error:\nReconectando em {failed_attempts*MaxFailuresError.wait_time_seconds}', level=LogLevels.WARNING)
                failed_attempts += 1
                time.sleep(failed_attempts*MaxFailuresError.wait_time_seconds)
    return reattempt_execution

class BasePath:
    debug = False
    batch_size = 200000
    regular_sleep_time_seconds = 5
    progress_tracker: ProgressTracker = ProgressTracker()
    path:str = ''
    name: str = ''

    @load_path_and_name
    def __init__(self, path: str = None, name: str = None, *args, **kwargs):
        if path and name:
            self.name = name.replace(' ','_').replace(':','')
            self.load_path_variable(path=path)

    @property
    def full_path(self):
        if not self.path:
            raise InvalidPathError(object=self)
        if not self.name:
            return self.path
        if hasattr(self, 'database') and self.database:
            if self.database.feature_dataset:
                return os.path.join(self.database.feature_dataset_full_path, self.name)
        return os.path.join(self.path, self.name)

    def format_date_as_str(self, current_date: datetime, return_format: str = "%Y-%m-%dT%H:%M:%S"):
        """Formats a datetime object on the format 1995/10/13T00:00:00"""
        if isinstance(current_date, datetime):
            return datetime.strftime(current_date, return_format)
        if isinstance(current_date, date):
            return datetime.strftime(current_date, return_format)

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
        return self.format_date_as_str(self.today, return_format='%Y%m%d')
    
    @property
    def exists(self):
        return Exists(self.full_path)

    def load_path_variable(self, path: str, subsequent_folders: list = []) -> str:
        """Loads a path variable and guarantees it exists and is accessible
            Args:
                path (str): Folder path string
                subsequent_folders (list, optional): Next folders to be acessed. Defaults to [].
            Raises:
                FolderAccessError
            Returns:
                str: Compiled folder path string
        """
        if not os.path.exists(path) and not Exists(path):
            try:
                os.makedirs(path)
            except Exception as e:
                raise FolderAccessError(folder=path, error=e)

        if subsequent_folders and not isinstance(subsequent_folders, list):
            subsequent_folders = [subsequent_folders]

        for subsequent_folder in subsequent_folders:
            path = os.path.join(path, subsequent_folder)
            if not os.path.exists(path) and not Exists(path):
                try:
                    os.makedirs(path)
                except Exception as e:
                    raise FolderAccessError(folder=path, error=e)

        self.path = path
        return path

    @staticmethod
    def get_list_of_valid_paths(items) -> list:
        valid_paths = []
        if isinstance(items, list):
            for item in items:
                path = item
                if hasattr(item, 'full_path'):
                    path = item.full_path
                if Exists(path):
                    valid_paths.append(path)
        elif items.exists and hasattr(items, 'full_path'):
            valid_paths.append(items.full_path)
        
        if valid_paths:
            return valid_paths
        else:
            raise UnexistingFeatureError(feature=items)
        
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
            aprint(f"O arquivo {name} está corrompido", level=LogLevels.WARNING)
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
                aprint(f"Pasta {folder} vazia", level=LogLevels.WARNING)
        elif files:
            for item in files:
                extracted_list.append(self.unzip_file(path=folder, name=item))

        return [item for item in extracted_list if item]
