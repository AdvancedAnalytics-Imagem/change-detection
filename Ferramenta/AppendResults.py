# -*- coding: utf-8 -*-
#!/usr/bin/python
import datetime
from datetime import datetime, timedelta

from arcpy import GetParameter, GetParameterAsText, GetParameterInfo

from core._logs import *
from core.adapters.SateliteImagery import ImageAcquisition
from core.configs.Configs import Configs
from core.instances.Images import Image
from core.libs.BaseProperties import BaseProperties
from core.ml_models.ImageClassifier import BaseImageClassifier


def load_arcgis_variables(variables_obj: Configs) -> Configs:
    current_image_date = datetime.now()
    historic_image_date = datetime.now() - timedelta(days=30)
    processing_date = datetime.now()

    if len(GetParameterInfo())>0:
        variables_obj.debug = False
        variables_obj.arcgis_execution = True
    else:
        variables_obj.debug = True

    if variables_obj.arcgis_execution:
        #* Imagem atual
        change_detection = GetParameterAsText(0)
        if change_detection and variables_obj.change_detection != change_detection:
            variables_obj.change_detection = Feature(path=change_detection)

        #* Data da imagem atual
        current_image_date = GetParameterAsText(1)
        if current_image_date and variables_obj.current_image_date != current_image_date:
            variables_obj.current_image_date = current_image_date
        
        #* Data da imagem histórica
        historic_image_date = GetParameterAsText(2)
        if historic_image_date and variables_obj.historic_image_date != historic_image_date:
            variables_obj.historic_image_date = historic_image_date
        
        #* Data do processamento
        processing_date = GetParameterAsText(3)
        if processing_date and variables_obj.processing_date != processing_date:
            variables_obj.processing_date = processing_date
        
        #* Classificação atual
        current_classification = GetParameterAsText(3)
        if current_classification and variables_obj.current_classification != current_classification:
            variables_obj.current_classification = Feature(path=current_classification)
        
        #* Classificação histórica
        historic_classification = GetParameterAsText(3)
        if historic_classification and variables_obj.historic_classification != historic_classification:
            variables_obj.historic_classification = Feature(path=historic_classification)

    return variables_obj.init_base_variables()

VARIABLES = load_arcgis_variables(variables_obj=Configs())
BASE_CONFIGS = BaseProperties()

class AppendResults:
    def __init__(self, variables: Configs, configs: BaseProperties):
        self.variables = variables
        self.configs = configs

    def append_data(self):
        tile_names = ', '.join(images.service.tile_names)
        if self.variables.current_classification:
            self.variables.classificacao_atual.append_dataset(
                origin=self.variables.current_classification,
                extra_constant_values={
                    'DATA':self.variables.current_image_date,
                    'DATA_PROC':self.variables.processing_date,
                    'SENSOR':self.variables.sensor,
                    'TILES':tile_names
                }
            )
        if self.variables.historic_classification:
            self.variables.classificacao_historica.append_dataset(
                origin=self.variables.historic_classification,
                extra_constant_values={
                    'DATA':self.variables.historic_image_date,
                    'DATA_PROC':self.variables.processing_date,
                    'SENSOR':self.variables.sensor,
                    'TILES':tile_names
                }
            )
        if self.variables.change_detection:
            self.variables.deteccao_de_mudancas.append_dataset(
                origin=self.variables.change_detection,
                extra_constant_values={
                    'DATA_A':self.variables.current_image_date,
                    'DATA_H':self.variables.historic_image_date,
                    'DATA_PROC':self.variables.processing_date,
                    'SENSOR':self.variables.sensor,
                    'TILES':tile_names
                }
            )
        
if __name__ == '__main__':
    detected_changes = AppendResults(variables=VARIABLES, configs=BASE_CONFIGS).append_data()
    aprint(f'''\n_ _______________________ _
               \nAppend de dados concluído com sucesso, resultados podem ser encontradas em:
               \n{detected_changes.full_path}
    ''')

    BASE_CONFIGS.delete_temporary_content()
