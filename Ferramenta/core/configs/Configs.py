# -*- coding: utf-8 -*-
#!/usr/bin/python
import json
import os

import yaml
from arcpy import GetParameter, GetParameterAsText, GetParameterInfo
from core._constants import *
from core._logs import *
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.instances.MosaicDataset import MosaicDataset
from core.libs.Base import BasePath
from core.libs.ErrorManager import VariablesLoadingError


class Configs(BasePath):
    def __init__(self) -> None:
        self.load_all_variables()
        self.init_base_variables()

    def init_base_variables(self):
        if hasattr(self, 'target_area') and self.target_area:
            if not isinstance(self.target_area, Feature):
                self.target_area = Feature(path=self.target_area)
            
        if hasattr(self, 'classificacao_atual') and self.classificacao_atual:
            if not isinstance(self.classificacao_atual, Feature):
                self.classificacao_atual = Feature(path=self.classificacao_atual)
            
        if hasattr(self, 'classificacao_historica') and self.classificacao_historica:
            if not isinstance(self.classificacao_historica, Feature):
                self.classificacao_historica = Feature(path=self.classificacao_historica)
            
        if hasattr(self, 'deteccao_de_mudancas') and self.deteccao_de_mudancas:
            if not isinstance(self.deteccao_de_mudancas, Feature):
                self.deteccao_de_mudancas = Feature(path=self.deteccao_de_mudancas)

        if hasattr(self, 'output_mosaic_dataset_current') and self.output_mosaic_dataset_current:
            if not isinstance(self.output_mosaic_dataset_current, MosaicDataset):
                self.output_mosaic_dataset_current = MosaicDataset(path=self.output_mosaic_dataset_current)

        if hasattr(self, 'output_mosaic_dataset_historic') and self.output_mosaic_dataset_historic:
            if not isinstance(self.output_mosaic_dataset_historic, MosaicDataset):
                self.output_mosaic_dataset_historic = MosaicDataset(path=self.output_mosaic_dataset_historic)

        if hasattr(self, 'download_storage') and self.download_storage:
            os.environ['DOWNLOAD_STORAGE'] = self.download_storage

        if hasattr(self, 'image_storage') and self.image_storage:
            os.environ['IMAGE_STORAGE'] = self.image_storage
        
        if hasattr(self, 'temp_dir') and self.temp_dir:
            os.environ['TEMP_DIR'] = self.temp_dir
        
        if hasattr(self, 'temp_db') and self.temp_db:
            os.environ['TEMP_DB'] = self.temp_db
        
        if hasattr(self, 'delete_temp_files') and self.delete_temp_files:
            os.environ['DELETE_TEMP_FILES'] = 'True'
        
        if hasattr(self, 'delete_temp_files_while_processing') and self.delete_temp_files_while_processing:
            os.environ['DELETE_TEMP_FILES_WHILE_PROCESSING'] = 'True'
        
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
