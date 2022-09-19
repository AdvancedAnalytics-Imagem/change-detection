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
        variables_obj.arcgis_execution = False

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
        img_acqstn_adptr = ImageAcquisition(
            service=self.variables.sensor, # TODO Check sensor type/string
        )

        tile_names = ', '.join(img_acqstn_adptr.service.get_selected_tiles_names(area_of_interest=self.variables.change_detection))

        if self.variables.change_detection and self.variables.change_detection_dest:
            self.append_change_detection(
                origin=self.variables.change_detection,
                destination=self.variables.change_detection_dest,
                tile_names=tile_names
            )

        if self.variables.current_classification and self.variables.current_classification_dest:
            self.append_current_classification(
                origin=self.variables.current_classification,
                destination=self.variables.current_classification_dest,
                tile_names=tile_names
            )

        if self.variables.historic_classification and self.variables.historic_classification_dest:
            self.append_historic_classification(
                origin=self.variables.historic_classification,
                destination=self.variables.historic_classification_dest,
                tile_names=tile_names
            )

        aprint('\n_ _______________________ _\nAppend de dados concluído com sucesso')


    def append_current_classification(self, origin: Feature, destination: Feature, tile_names: str):
        destination.append_dataset(
            origin=origin,
            extra_constant_values={
                'data':self.variables.current_image_date,
                'data_proc':self.variables.processing_date,
                'sensor':self.variables.sensor,
                'tiles':tile_names
            }
        )
        aprint('Append de dados atuais concluído com sucesso')

    def append_historic_classification(self, origin: Feature, destination: Feature, tile_names: str):
        destination.append_dataset(
            origin=origin,
            extra_constant_values={
                'data':self.variables.historic_image_date,
                'data_proc':self.variables.processing_date,
                'sensor':self.variables.sensor,
                'tiles':tile_names
            }
        )
        aprint('Append de dados histórico concluído com sucesso')

    def append_change_detection(self, origin: Feature, destination: Feature, tile_names: str):
        destination.append_dataset(
            origin=origin,
            extra_constant_values={
                'dataimgatual':self.variables.current_image_date,
                'dataimghist':self.variables.historic_image_date,
                'dataprocessamento':self.variables.processing_date,
                'sensor_a':self.variables.sensor,
                'tiles_a':tile_names
            },
            field_map={
                'class':'class_h',
                'class_1':'class_a',
            }
        )
        aprint('Append de dados de classificação concluído com sucesso')

        
if __name__ == '__main__':
    AppendResults(variables=VARIABLES, configs=BASE_CONFIGS).append_data()

    BASE_CONFIGS.delete_temporary_content()
