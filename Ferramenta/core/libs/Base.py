# -*- encoding: utf-8 -*-
#!/usr/bin/python
import os
import time

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
                if failed_attempts > 20:
                    raise MaxFailuresError(wrapped_function.__name__, attempts=failed_attempts)
                aprint(f'Sentinel Server error:\n{e}\nReattempting connection in a few...', level=LogLevels.WARNING)
                failed_attempts += 1
                time.sleep(failed_attempts*20)

    return reattempt_execution

class BasePath:
    path:str = ''
    name: str = ''

    @load_path_and_name
    def __init__(self, path: str = None, name: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if path and name:
            name = name.replace(' ','_').replace(':','')
            self.name = name
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
        
    @property
    def exists(self):
        return Exists(self.full_path)
        
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
