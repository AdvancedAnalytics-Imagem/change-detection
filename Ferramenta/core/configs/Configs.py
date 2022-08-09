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

        if not self.download_storage:
            self.download_storage = DOWNLOADS_DIR

        if self.insert_on_database:
            self.classificacao_atual = Feature(path=self.classificacao_atual, temp_destination=self.temp_db)
            self.classificacao_historica = Feature(path=self.classificacao_historica, temp_destination=self.temp_db)
            self.deteccao_de_mudancas = Feature(path=self.deteccao_de_mudancas, temp_destination=self.temp_db)
            
        self.target_area = Feature(path=self.target_area, temp_destination=self.temp_db)

        self.output_images_location = Database(path=self.output_images_location)

        self.output_mosaic_dataset_current = MosaicDataset(path=self.output_mosaic_dataset_current, create=True)
        self.output_mosaic_dataset_historic = MosaicDataset(path=self.output_mosaic_dataset_historic, create=True)


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
                    self.__dict__.update(json.load(f))

                if file.endswith('.yaml'):
                    self.__dict__.update(yaml.safe_load(f))

        self.load_arcgis_variables()
    
    def load_config_files(self):
        return [
            *self.get_files_by_extension(folder=CONFIGS_DIR, extension='.json'),
            *self.get_files_by_extension(folder=CONFIGS_DIR, extension='.yaml'),
        ]

    def load_arcgis_variables(self):
        if len(GetParameterInfo())>0:
            aprint(message='Execução no ArcGIS', level=LogLevels.WARNING)
            self.debug = False
            self.arcgis_execution = True
        else:
            aprint(message='Execução fora do ArcGIS', level=LogLevels.WARNING)
            self.debug = True

        if self.arcgis_execution:
            target_area = GetParameterAsText(0)
            if target_area and self.target_area != target_area:
                self.target_area = target_area

            deteccao_de_mudancas = GetParameterAsText(1)
            if deteccao_de_mudancas and self.deteccao_de_mudancas != deteccao_de_mudancas:
                self.deteccao_de_mudancas = deteccao_de_mudancas

            classificacao_atual = GetParameterAsText(2)
            if classificacao_atual and self.classificacao_atual != classificacao_atual:
                self.classificacao_atual = classificacao_atual

            classificacao_historica = GetParameterAsText(3)
            if classificacao_historica and self.classificacao_historica != classificacao_historica:
                self.classificacao_historica = classificacao_historica

            output_images_location = GetParameterAsText(4)
            if output_images_location and self.output_images_location != output_images_location:
                self.output_images_location = output_images_location

            output_mosaic_dataset_current = GetParameterAsText(5)
            if output_mosaic_dataset_current and self.output_mosaic_dataset_current != output_mosaic_dataset_current:
                self.output_mosaic_dataset_current = output_mosaic_dataset_current

            output_mosaic_dataset_historic = GetParameterAsText(6)
            if output_mosaic_dataset_historic and self.output_mosaic_dataset_historic != output_mosaic_dataset_historic:
                self.output_mosaic_dataset_historic = output_mosaic_dataset_historic

            max_cloud_coverage = GetParameter(7)
            if max_cloud_coverage and self.max_cloud_coverage != max_cloud_coverage:
                self.max_cloud_coverage = max_cloud_coverage

            temp_dir = GetParameter(8)
            if temp_dir and self.temp_dir != temp_dir:
                self.temp_dir = temp_dir

            download_storage = GetParameter(9)
            if download_storage and self.download_storage != download_storage:
                self.download_storage = download_storage

            delete_temp_files = GetParameter(10)
            if delete_temp_files and self.delete_temp_files != delete_temp_files:
                self.delete_temp_files = delete_temp_files

            classification_processor = GetParameterAsText(11)
            if classification_processor and self.classification_processor != classification_processor:
                self.classification_processor = classification_processor

            classification_arguments = GetParameterAsText(12)
            if classification_arguments and self.classification_arguments != classification_arguments:
                self.classification_arguments = classification_arguments
