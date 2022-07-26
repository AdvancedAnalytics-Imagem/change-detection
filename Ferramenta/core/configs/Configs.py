# -*- coding: utf-8 -*-
#!/usr/bin/python
import json
import os

from core._constants import *
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.libs.Base import BasePath


class Configs(BasePath):
    temp: str

    def __init__(self) -> None:
        self.load_variables()
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

        
    def load_variables(self):
        variables = {}
        files = self.load_config_files()
        for file in files:
            filename = os.path.splitext(os.path.basename(file))[0]
            with open(file) as f:
                self.__dict__.update(json.load(f))
        

    def load_config_files(self):
        return self.get_files_by_extension(folder=CONFIGS_DIR, extension='.json')