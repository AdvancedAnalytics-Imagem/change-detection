# -*- coding: utf-8 -*-
#!/usr/bin/python
import json
import os
from datetime import date

import yaml
from arcpy import GetParameter, GetParameterAsText, GetParameterInfo
from core._constants import *
from core._logs import *
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.instances.MosaicDataset import MosaicDataset
from core.libs.Base import BasePath
from core.libs.CustomExceptions import VariablesLoadingError


class Configs(BasePath):
    def __init__(self) -> None:
        self.load_all_variables()

    def init_base_variables(self):
        if hasattr(self, 'target_area') and self.target_area:
            if not isinstance(self.target_area, Feature):
                self.target_area = Feature(path=self.target_area)
            
        if hasattr(self, 'current_classification_dest') and self.current_classification_dest:
            if not isinstance(self.current_classification_dest, Feature):
                self.current_classification_dest = Feature(path=self.current_classification_dest)
            
        if hasattr(self, 'historic_classification_dest') and self.historic_classification_dest:
            if not isinstance(self.historic_classification_dest, Feature):
                self.historic_classification_dest = Feature(path=self.historic_classification_dest)
            
        if hasattr(self, 'change_detection_dest') and self.change_detection_dest:
            if not isinstance(self.change_detection_dest, Feature):
                self.change_detection_dest = Feature(path=self.change_detection_dest)

        if hasattr(self, 'output_mosaic_dataset_current') and self.output_mosaic_dataset_current:
            if not isinstance(self.output_mosaic_dataset_current, MosaicDataset):
                self.output_mosaic_dataset_current = MosaicDataset(path=self.output_mosaic_dataset_current)

        if hasattr(self, 'output_mosaic_dataset_historic') and self.output_mosaic_dataset_historic:
            if not isinstance(self.output_mosaic_dataset_historic, MosaicDataset):
                self.output_mosaic_dataset_historic = MosaicDataset(path=self.output_mosaic_dataset_historic)

        if hasattr(self, 'image_storage') and self.image_storage:
            if not os.environ.get('IMAGE_STORAGE'):
                os.environ['IMAGE_STORAGE'] = self.image_storage
        
        if hasattr(self, 'temp_dir') and self.temp_dir:
            if not os.environ.get('TEMP_DIR'):
                os.environ['TEMP_DIR'] = self.temp_dir
            if not os.environ.get('TEMP_DB'):
                os.environ['TEMP_DB'] = os.path.join(self.temp_dir, f'{os.path.basename(self.temp_dir)}.gdb')

        if hasattr(self, 'download_storage') and self.download_storage:
            if not os.environ.get('DOWNLOAD_STORAGE'):
                os.environ['DOWNLOAD_STORAGE'] = self.download_storage

        if hasattr(self, 'delete_temp_files') and self.delete_temp_files:
            if not os.environ.get('DELETE_TEMP_FILES'):
                os.environ['DELETE_TEMP_FILES'] = 'True'

        if hasattr(self, 'delete_temp_files_while_processing') and self.delete_temp_files_while_processing:
            if not os.environ.get('DELETE_TEMP_FILES_WHILE_PROCESSING'):
                os.environ['DELETE_TEMP_FILES_WHILE_PROCESSING'] = 'True'

        if hasattr(self, 'use_arcpy_append') and self.use_arcpy_append:
            if not os.environ.get('USE_ARCPY_APPEND'):
                os.environ['USE_ARCPY_APPEND'] = 'True'
        
        if hasattr(self, 'ml_model') and self.ml_model:
            if not os.environ.get('ML_MODEL'):
                os.environ['ML_MODEL'] = self.ml_model

        if hasattr(self, 'max_date') and self.max_date:
            if not isinstance(self.max_date, date):
                self.max_date = datetime.strptime(self.max_date, '%Y-%m-%d')

        return self
        

    def get(self, key):
        return self.__dict__.get(key)

    @property
    def _keys(self):
        return self.__dict__.keys()

    def load_all_variables(self):
        variables = {}
        files = self.load_config_files()
        for file in files:
            filename = os.path.splitext(os.path.basename(file))[0]
            with open(file) as f:
                if file.endswith('.json'):
                    self.update(json_vars=f)
                if file.endswith('.yaml'):
                    self.update(yaml_vars=f)

    def update(self, json_vars: str = '', yaml_vars: str = '', vars: dict = {}):
        if json_vars:
            try:
                self.__dict__.update(json.load(json_vars))
            except Exception as e:
                VariablesLoadingError(variables=json_vars)
        if yaml_vars:
            try:
                self.__dict__.update(yaml.safe_load(yaml_vars))
            except Exception as e:
                VariablesLoadingError(variables=yaml_vars)
        if vars:
            try:
                self.__dict__.update(vars)
            except Exception as e:
                VariablesLoadingError(variables=vars)

    def load_config_files(self):
        return [
            *self.get_files_by_extension(folder=CONFIGS_DIR, extension='.json'),
            *self.get_files_by_extension(folder=CONFIGS_DIR, extension='.yaml'),
        ]
