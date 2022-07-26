# -*- encoding: utf-8 -*-
#!/usr/bin/python
import os
import shutil

from arcpy import Exists
from arcpy.management import Delete
from core._constants import *
from core._logs import *
from core.instances.Database import Database
from core.libs.Base import BasePath
from core.libs.CustomExceptions import DeletionError


class BaseProperties(BasePath):
    _temp_db: Database = None
    _image_storage: str = None
    
    @property
    def delete_temp_files_while_processing(self) -> bool:
        return os.environ.get('DELETE_TEMP_FILES_WHILE_PROCESSING', False) == 'True'

    @property
    def delete_temp_files(self) -> bool:
        return os.environ.get('DELETE_TEMP_FILES', False) == 'True'

    @property
    def temp_dir(self) -> str:
        return os.environ.get('TEMP_DIR', TEMP_DIR)

    @property
    def download_storage(self) -> str:
        return os.environ.get('DOWNLOAD_STORAGE', DOWNLOADS_DIR)

    @property
    def image_storage(self) -> str:
        if not self._image_storage:
            image_storage = os.environ.get('IMAGE_STORAGE', IMAGES_DIR)
            if '.gdb' in image_storage or '.sde' in image_storage:
                self._image_storage = Database(path=image_storage)
            else:
                self._image_storage = image_storage
        return self._image_storage

    @property
    def temp_db(self) -> Database:
        if not self._temp_db:
            self._temp_db = Database(path=os.environ.get('TEMP_DB', 'IN_MEMORY'))
        return self._temp_db

    @property
    def n_cores(self) -> int:
        n_cores = int(os.environ.get('N_CORES', 0))
        if not n_cores:
            n_cores = os.cpu_count()
        return n_cores
        
    @property
    def use_arcpy_append(self) -> bool:
        return os.environ.get('USE_ARCPY_APPEND', 'True') == 'True'

    @property
    def ml_model(self) -> str:
        return os.environ.get('ML_MODEL')

    def delete_temporary_content(self) -> None:
        if self.delete_temp_files:
            if Exists(self.temp_dir):
                try:
                    aprint(f'Removendo arquivos temporários de processamento:\n{self.temp_dir}')
                    Delete(self.temp_dir)
                except Exception as e:
                    DeletionError(path=self.temp_dir)
            if Exists(self.temp_db):
                try:
                    Delete(self.temp_db)
                except Exception as e:
                    DeletionError(path=self.temp_db)
        
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            
    @property
    def sensor(self) -> str:
        return os.environ.get('SENSOR', 'SENTINEL2')