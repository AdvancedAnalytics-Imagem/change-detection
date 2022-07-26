# -*- coding: utf-8 -*-
#!/usr/bin/python
import os
from enum import Enum, unique

from core._constants import *
from core.libs.BaseProperties import BaseProperties

class ExtendedEnum(Enum):
    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))

class ClassAttribute:
    def __init__(self, value: int, label: str):
        self.value = value
        self.label = label
    
    def __str__(self):
        return self.label

class BaseImageClassifier(BaseProperties):
    ml_model_extension: str = '.dlpk'
    ml_model_name: str = ''
    class_field: str = 'CLASS'

    class Classes(ExtendedEnum):
        pass
    
    def get_ml_model(self, target: str) -> str:
        if self.ml_model: return self.ml_model
        
        list_of_models = self.get_files_by_extension(folder=ML_MODELS_DIR, extension=self.ml_model_extension)
        target_files = [file for file in list_of_models if target in file]
        return target_files[0]

    @property
    def ids(self):
        return [id for id in self.Classes]

class Sentinel2ImageClassifier(BaseImageClassifier):
    ml_model_name: str = 'sentinel2'

    class Classes(ExtendedEnum):
        AREA_ANTROPICA = ClassAttribute(60, "Área Antrópicas Não Agrícolas")
        AREA_CAMPESTRE = ClassAttribute(10, "Campestres")
        CULTURA_PERENE = ClassAttribute(20, "Cultura Permanente")
        CULTURA_TEMPORARIA = ClassAttribute(30, "Cultura Temporária")
        AREA_FLORESTAL = ClassAttribute(40, "Florestal")
        MASSA_DAGUA = ClassAttribute(50, "Massa D’água")
        OTHER = ClassAttribute(0, "Outros")

    def __init__(self):
        super().__init__(path=self.get_ml_model(target=self.ml_model_name))

class CbersImageClassifier(BaseImageClassifier):
    ml_model_name: str = 'cbers'

    class Classes(ExtendedEnum):
        AREA_ANTROPICA = ClassAttribute(60, "Área Antrópicas Não Agrícolas")
        AREA_CAMPESTRE = ClassAttribute(10, "Campestres")
        CULTURA_PERENE = ClassAttribute(20, "Cultura Permanente")
        CULTURA_TEMPORARIA = ClassAttribute(30, "Cultura Temporária")
        AREA_FLORESTAL = ClassAttribute(40, "Florestal")
        MASSA_DAGUA = ClassAttribute(50, "Massa D’água")
        OTHER = ClassAttribute(0, "Outros")

    def __init__(self):
        super().__init__(path=self.get_ml_model(target=self.ml_model_name))
