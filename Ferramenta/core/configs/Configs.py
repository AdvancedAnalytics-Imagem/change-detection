# -*- coding: utf-8 -*-
#!/usr/bin/python
import json
import os

from arcpy import GetParameter, GetParameterInfo
from core._constants import *
from core._logs import *
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.libs.Base import BasePath


class Configs(BasePath):
    arcgis_execution: bool = False

    def __init__(self) -> None:
        self.load_all_variables()

        if not self.temp_dir:
            self.temp_dir = TEMP_DIR

        if not self.temp_db:
            self.temp_db = Database(path=TEMP_DB)
        else:
            self.temp_db = Database(path=self.temp_db)

        if not self.downloads_storage:
            self.downloads_storage = DOWNLOADS_DIR

        if self.insert_on_database:
            self.classificacao_atual = Feature(path=self.classificacao_atual, temp_destination=self.temp_db)
            self.classificacao_historica = Feature(path=self.classificacao_historica, temp_destination=self.temp_db)
            self.deteccao_de_mudancas = Feature(path=self.deteccao_de_mudancas, temp_destination=self.temp_db)
            
        self.target_area = Feature(path=self.target_area, temp_destination=self.temp_db)

        self.output_images_location = Database(path=self.output_images_location)

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
                self.__dict__.update(json.load(f))

        self.load_arcgis_variables()

        for key in self._keys:
            aprint(f'{key} -> {self.get(key)}', display_message=True, level='info')
    
    def load_arcgis_variables(self):
        if len(GetParameterInfo())>0:
            aprint(message='Execução no ArcGIS', level='warning')
            self.debug = False
            self.arcgis_execution = True
        else:
            aprint(message='Execução fora do ArcGIS', level='warning')
            self.debug = True

        if self.arcgis_execution or True:
            self.target_area = GetParameter(0)            # 0 - Executar o Log de Arquivos de interesse?
            self.deteccao_de_mudancas = GetParameter(1)            # 0 - Executar o Log de Arquivos de interesse?
            self.classificacao_atual = GetParameter(2)            # 0 - Executar o Log de Arquivos de interesse?
            self.classificacao_historica = GetParameter(3)            # 0 - Executar o Log de Arquivos de interesse?
            self.output_images_location = GetParameter(4)            # 0 - Executar o Log de Arquivos de interesse?
            self.output_mosaic_dataset = GetParameter(5)            # 0 - Executar o Log de Arquivos de interesse?
            self.max_cloud_coverage = GetParameter(6)            # 0 - Executar o Log de Arquivos de interesse?
            self.temp_dir = GetParameter(7)            # 0 - Executar o Log de Arquivos de interesse?
            self.download_storage = GetParameter(8)            # 0 - Executar o Log de Arquivos de interesse?
            self.delete_temp_files = GetParameter(9)            # 0 - Executar o Log de Arquivos de interesse?

    def load_config_files(self):
        return self.get_files_by_extension(folder=CONFIGS_DIR, extension='.json')
