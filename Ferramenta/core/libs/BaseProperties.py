# -*- encoding: utf-8 -*-
#!/usr/bin/python
import os

from core._constants import *
from core._logs import *
from core.instances.Database import Database
from core.libs.Base import BasePath
from core.libs.ErrorManager import InvalidMLClassifierError


class BaseProperties(BasePath):
    _temp_db: Database = None
    
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
    def ml_processor(self) -> str:
        """Type of processor used for ML classification
            Returns:
                str: Either 'CPU' or 'GPU'
        """
        processor_type = os.environ.get('ML_PROCESSOR_TYPE', 'CPU')
        if processor_type not in ['CPU', 'GPU']:
            raise InvalidMLClassifierError(p_type=processor_type)
        return processor_type

    @property
    def n_cores(self) -> int:
        return int(os.environ.get('N_CORES', os.cpu_count()))
