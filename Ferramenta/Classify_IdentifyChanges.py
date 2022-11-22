# -*- coding: utf-8 -*-
#!/usr/bin/python
import datetime
from datetime import datetime

from arcpy import GetParameter, GetParameterAsText, GetParameterInfo, Exists

from core._logs import *
from core.adapters.SateliteImagery import ImageAcquisition
from core.configs.Configs import Configs
from core.instances.Feature import Feature
from core.instances.Images import Image
from core.libs.BaseProperties import BaseProperties
from core.ml_models.ImageClassifier import BaseImageClassifier
from core.libs.CustomExceptions import InvalidMLClassifierError


def load_arcgis_variables(variables_obj: Configs) -> Configs:
    variables_obj.current_image = None
    variables_obj.historic_image = None

    if len(GetParameterInfo())>0:
        variables_obj.debug = False
        variables_obj.arcgis_execution = True
    else:
        variables_obj.debug = True
        variables_obj.arcgis_execution = False

    if variables_obj.arcgis_execution:
        #* Imagem atual
        current_image = GetParameterAsText(0)
        if current_image:
            variables_obj.current_image = Image(path=current_image)

        #* Data da imagem atual
        current_image_date = GetParameterAsText(1)
        if current_image_date:
            variables_obj.current_image.date_created = current_image_date
        
        #* Imagem histórica
        historic_image = GetParameterAsText(2)
        if historic_image:
            variables_obj.historic_image = Image(path=historic_image)
        
        #* Data da imagem histórica
        historic_image_date = GetParameterAsText(3)
        if historic_image_date:
            variables_obj.historic_image.date_created = historic_image_date
        
        #* Parâmetros de classificação
        classification_arguments = GetParameterAsText(4)
        if classification_arguments and variables_obj.classification_arguments != classification_arguments:
            variables_obj.classification_arguments = classification_arguments

        classification_processor = GetParameterAsText(5)
        if classification_processor and variables_obj.classification_processor != classification_processor:
            if classification_processor not in ['CPU', 'GPU']:
                raise InvalidMLClassifierError(p_type=classification_processor)
            variables_obj.classification_processor = classification_processor

        n_cores = GetParameterAsText(6)
        if n_cores:
            os.environ['N_CORES'] = n_cores

        #* Armazenamento temporario (Opcional - IN_MEMORY)
        temp_dir = GetParameterAsText(7)
        if temp_dir:
            os.environ['TEMP_DIR'] = temp_dir
            os.environ['TEMP_DB'] = os.path.join(temp_dir, f'{os.path.basename(temp_dir)}.gdb')

        #* Deletar arquivos temporarios (Opcional - False)
        delete_temp_files = GetParameter(8)
        if delete_temp_files:
            os.environ['DELETE_TEMP_FILES'] = 'True'
            
        ml_model = GetParameterAsText(9)
        if ml_model:
            if Exists(ml_model):
                if ml_model.endswith('.dlpk') or ml_model.endswith('.emd'):
                    os.environ['ML_MODEL'] = ml_model
            else:
                aprint(f'Modelo de deep learning não foi especificado, o modelo padrão será utilizado.')
                aprint(f'Modelo de deep learning não foi especificado, o modelo padrão será utilizado.')
                

    return variables_obj.init_base_variables()

VARIABLES = load_arcgis_variables(variables_obj=Configs())
BASE_CONFIGS = BaseProperties()

class ClassifyAndDetectChanges:
    def __init__(self, variables: Configs, configs: BaseProperties):
        self.variables = variables
        self.configs = configs
    
    def classify_and_detect_changes(self):        
        #* Stating Sensor Service Adapter
        image_acquisition_adapter = ImageAcquisition(
            service=self.variables.sensor, # TODO Check sensor type/string
            credentials=self.variables.credentials
        )

        if VARIABLES.current_image:
            curr_classif = self.classify_image(
                image=self.variables.current_image,
                ml_model=image_acquisition_adapter.service.ml_model
            )
            aprint(f'Classificação da imagem atual:\nf{curr_classif.full_path}')
        if VARIABLES.historic_image:
            hist_classif = self.classify_image(
                image=self.variables.historic_image,
                ml_model=image_acquisition_adapter.service.ml_model
            )
            aprint(f'Classificação da imagem histórica:\nf{hist_classif.full_path}')

        if VARIABLES.current_image and VARIABLES.historic_image:
            change_detection = self.detect_changes(current=curr_classif, historic=hist_classif)
            aprint(f'''\n_ _______________________ _
                        \nDetecção de mudançãs concluída com sucesso, mudanças detectadas podem ser encontradas em:
                        \n{detected_changes.full_path}
                    ''')
    
    def classify_image(self, image: Image = None, ml_model: BaseImageClassifier = None):
        aprint(message='Classificando Imagens', progress=True)
        if not image or not ml_model: return
        classification = image.classify(
            classifier=ml_model,
            output_path=BASE_CONFIGS.temp_db,
            arguments=VARIABLES.classification_arguments,
            processor_type=VARIABLES.classification_processor,
            n_cores=VARIABLES.n_cores
        )
        return classification

    def detect_changes(self, current, historic):
        aprint(message='Detectando Mudanças', progress=True)
        change_detection = Feature(path=current.intersects(intersecting_feature=historic))
        change_detection.calculate_field(
            field_name="DIFF",
            field_value=int,
            expression="diff(!gridcode!,!gridcode_1!)",
            code_block="""def diff(grid, grid1):
                    if grid == grid1:
                        return 0
                    else:
                        difference = grid - grid1 
                        return difference/abs(difference)""",
        )
        change_detection.calculate_field(
            field_name="CHANGES",
            field_value=str,
            expression=f"changes(!CLASS!,!CLASS_1!)",
            code_block="""def changes(classif, classif1):
                    return f'{classif} - {classif1}'"""
        )
        return change_detection

if __name__ == '__main__':
    aprint(
        message=f'''\n_ _________________ _\n
    1. Arquivos temporários serão salvos em: {BASE_CONFIGS.temp_dir}
    2. GeoDatabase temporário: {BASE_CONFIGS.temp_db.full_path}\n_ _________________ _\n'''
    )
    
    ClassifyAndDetectChanges(variables=VARIABLES, configs=BASE_CONFIGS).classify_and_detect_changes()
    BASE_CONFIGS.delete_temporary_content()
