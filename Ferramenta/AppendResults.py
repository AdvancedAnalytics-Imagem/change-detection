# -*- coding: utf-8 -*-
#!/usr/bin/python
import datetime
from datetime import datetime, timedelta

from arcpy import GetParameter, GetParameterAsText, GetParameterInfo

from core._logs import *
from core.adapters.SateliteImagery import ImageAcquisition
from core.configs.Configs import Configs
from core.instances.Feature import Feature
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
        #* Classificação atual
        change_detection = GetParameterAsText(0)
        if change_detection:
            variables_obj.change_detection = Feature(path=change_detection)

        #* Data da imagem atual
        current_image_date = GetParameterAsText(1)
        if current_image_date:
            variables_obj.current_image_date = current_image_date
        
        #* Data da imagem histórica
        historic_image_date = GetParameterAsText(2)
        if historic_image_date:
            variables_obj.historic_image_date = historic_image_date
        
        #* Data do processamento
        processing_date = GetParameterAsText(3)
        if processing_date:
            variables_obj.processing_date = processing_date

        #* Destino da classificação atual
        change_detection_dest = GetParameterAsText(4)
        if change_detection_dest:
            variables_obj.change_detection_dest = Feature(path=change_detection_dest)
        
        #* Classificação atual
        current_classification = GetParameterAsText(5)
        if current_classification:
            variables_obj.current_classification = Feature(path=current_classification)
        
        #* Destino da Classificação atual
        current_classification_dest = GetParameterAsText(6)
        if current_classification_dest:
            variables_obj.current_classification_dest = Feature(path=current_classification_dest)
        
        #* Classificação histórica
        historic_classification = GetParameterAsText(7)
        if historic_classification:
            variables_obj.historic_classification = Feature(path=historic_classification)
            
        #* Destino da Classificação histórica
        historic_classification_dest = GetParameterAsText(8)
        if historic_classification_dest:
            variables_obj.historic_classification_dest = Feature(path=historic_classification_dest)

    return variables_obj.init_base_variables()

VARIABLES = load_arcgis_variables(variables_obj=Configs())
BASE_CONFIGS = BaseProperties()

class AppendResults:
    def __init__(self, variables: Configs, configs: BaseProperties):
        self.variables = variables
        self.configs = configs

    def append_data(self):
        tile_names = ', '.join(images.service.tile_names)
        image_acquisition_adapter = ImageAcquisition(
            service=self.variables.sensor, # TODO Check sensor type/string
            credentials=self.variables.sentinel_api_auth,
            downloads_folder=self.variables.download_storage
        )

        if self.variables.current_classification:
            self.variables.current_classification_dest.append_dataset(
                origin=self.variables.current_classification,
                extra_constant_values={
                    'DATA':self.variables.current_image_date,
                    'DATA_PROC':self.variables.processing_date,
                    'SENSOR':self.variables.sensor,
                    'TILES':tile_names
                }
            )
        if self.variables.historic_classification:
            self.variables.historic_classification_dest.append_dataset(
                origin=self.variables.historic_classification,
                extra_constant_values={
                    'DATA':self.variables.historic_image_date,
                    'DATA_PROC':self.variables.processing_date,
                    'SENSOR':self.variables.sensor,
                    'TILES':tile_names
                }
            )
        if self.variables.change_detection:
            self.variables.change_detection_dest.append_dataset(
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
