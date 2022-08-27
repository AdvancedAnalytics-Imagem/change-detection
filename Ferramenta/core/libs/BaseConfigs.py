# -*- encoding: utf-8 -*-
#!/usr/bin/python
import datetime
import os
import time
from datetime import date, datetime
from zipfile import ZipFile

from core._constants import *
from core._logs import *
from core.instances.Database import Database
from core.libs.Base import BasePath, load_path_and_name
from core.libs.ProgressTracking import ProgressTracker


class BaseConfigs(BasePath):
    debug = True
    _temp_db: Database = None
    batch_size = 200000
    regular_sleep_time_seconds = 5
    progress_tracker: ProgressTracker = ProgressTracker()

    def format_date_as_str(self, current_date: datetime, return_format: str = "%Y-%m-%dT%H:%M:%S"):
        """Formats a datetime object on the format 1995/10/13T00:00:00"""
        if isinstance(current_date, datetime):
            return datetime.strftime(current_date, return_format)
        if isinstance(current_date, date):
            return datetime.strftime(current_date, return_format)
    
    @property
    def delete_temp_files_while_processing(self) -> bool:
        if os.environ.get('DELETE_TEMP_FILES_WHILE_PROCESSING', False) == 'True':
            return True
        return False

    @property
    def delete_temp_files(self) -> bool:
        if os.environ.get('DELETE_TEMP_FILES', False) == 'True':
            return True
        return False

    @property
    def temp_dir(self) -> str:
        return os.environ.get('TEMP_DIR', TEMP_DIR)

    @property
    def download_storage(self) -> str:
        return os.environ.get('DOWNLOAD_STORAGE', DOWNLOADS_DIR)

    @property
    def image_storage(self) -> str:
        return os.environ.get('IMAGE_STORAGE', IMAGES_DIR)

    @property
    def temp_db(self) -> Database:
        if not self._temp_db:
            self._temp_db = Database(path=os.environ.get('TEMP_DB', 'IN_MEMORY'))
        return self._temp_db

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


class BaseDatabasePath(BaseConfigs):
    database: Database = None

    def __init__(self, path: str, name: str, create: bool = False, *args, **kwargs):
        super().__init__(path=path, name=name, *args, **kwargs)
        self.load_database_path_variable(path=path, name=name, create=create)

    @property
    def is_inside_database(self):
        return self.database is not None
        
    @load_path_and_name
    def load_database_path_variable(self, path: str, name: str = None, create: bool = False):
        """Loads a feature variable and guarantees it exists and, if in a GDB, if that GDB exists
            Args:
                path (str, optional): Path to the feature. Defaults to None.
                name (str): feature name
        """
        if self.path != 'IN_MEMORY':
            if create:
                if path.endswith('.sde') or path.endswith('.gdb'):
                    self.database = Database(path=path, create=create)
                else:
                    self.database = Database(path=path, name=name, create=create)

            if '.sde' in path or '.gdb' in path:
                self.database = Database(path=path)

            if self.database:
                self.path = self.database.full_path
