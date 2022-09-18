# -*- coding: utf-8 -*-
#!/usr/bin/python
import datetime
import os
from datetime import datetime

from arcpy import GetParameter, GetParameterAsText, GetParameterInfo

from core._logs import *
from core.adapters.SateliteImagery import ImageAcquisition
from core.configs.Configs import Configs
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.instances.Images import Image
from core.instances.MosaicDataset import MosaicDataset
from core.libs.BaseProperties import BaseProperties
from core.ml_models.ImageClassifier import BaseImageClassifier
from core.libs.ErrorManager import InvalidMLClassifierError

from AppendResults import AppendResults


def load_arcgis_variables(variables_obj):
    if len(GetParameterInfo())>0:
        variables_obj.debug = False
        variables_obj.arcgis_execution = True
    else:
        variables_obj.debug = True
        variables_obj.arcgis_execution = False

    if variables_obj.arcgis_execution:
        target_area = GetParameterAsText(0)
        if target_area and variables_obj.target_area != target_area:
            variables_obj.target_area = target_area

        change_detection_dest = GetParameterAsText(1)
        if change_detection_dest and variables_obj.change_detection_dest != change_detection_dest:
            variables_obj.change_detection_dest = change_detection_dest

        current_classification_dest = GetParameterAsText(2)
        if current_classification_dest and variables_obj.current_classification_dest != current_classification_dest:
            variables_obj.current_classification_dest = current_classification_dest

        historic_classification_dest = GetParameterAsText(3)
        if historic_classification_dest and variables_obj.historic_classification_dest != historic_classification_dest:
            variables_obj.historic_classification_dest = historic_classification_dest

        output_mosaic_dataset_current = GetParameterAsText(5)
        if output_mosaic_dataset_current and variables_obj.output_mosaic_dataset_current != output_mosaic_dataset_current:
            variables_obj.output_mosaic_dataset_current = output_mosaic_dataset_current

        output_mosaic_dataset_historic = GetParameterAsText(6)
        if output_mosaic_dataset_historic and variables_obj.output_mosaic_dataset_historic != output_mosaic_dataset_historic:
            variables_obj.output_mosaic_dataset_historic = output_mosaic_dataset_historic

        max_cloud_coverage = GetParameter(7)
        if max_cloud_coverage and variables_obj.max_cloud_coverage != max_cloud_coverage:
            variables_obj.max_cloud_coverage = max_cloud_coverage

        classification_processor = GetParameterAsText(10)
        if classification_processor and variables_obj.classification_processor != classification_processor:
            if classification_processor not in ['CPU', 'GPU']:
                raise InvalidMLClassifierError(p_type=classification_processor)
            variables_obj.classification_processor = classification_processor

        classification_arguments = GetParameterAsText(11)
        if classification_arguments and variables_obj.classification_arguments != classification_arguments:
            variables_obj.classification_arguments = classification_arguments

        image_storage = GetParameterAsText(4)
        if image_storage:
            os.environ['IMAGE_STORAGE'] = image_storage

        temp_dir = GetParameterAsText(8)
        if temp_dir:
            os.environ['TEMP_DIR'] = temp_dir
            os.environ['TEMP_DB'] = os.path.join(temp_dir, f'{os.path.basename(temp_dir)}.gdb')

        download_storage = GetParameterAsText(9)
        if download_storage:
            os.environ['DOWNLOAD_STORAGE'] = download_storage
            
        n_cores = GetParameterAsText(12)
        if n_cores:
            os.environ['N_CORES'] = str(variables_obj.n_cores)

        delete_temp_files = GetParameter(13)
        if delete_temp_files:
            os.environ['DELETE_TEMP_FILES'] = 'True'
        
        delete_temp_files_while_processing = GetParameter(14)
        if delete_temp_files_while_processing:
            os.environ['DELETE_TEMP_FILES_WHILE_PROCESSING'] = 'True'
            
    return variables_obj.init_base_variables()

VARIABLES = load_arcgis_variables(variables_obj=Configs())
BASE_CONFIGS = BaseProperties()

def get_images():
    aprint(message='Iniciando Aquisição de Imagens', progress=True)
    images = ImageAcquisition(
        service=VARIABLES.sensor,
        credentials=VARIABLES.credentials,
    )
    images.get_historic_and_current_images(
        area_of_interest=VARIABLES.target_area,
        results_output_location=BASE_CONFIGS.temp_db,
        max_cloud_coverage=VARIABLES.max_cloud_coverage,
        compose_as_single_image=VARIABLES.compose_as_single_image # Caso negativo, os tiles não serão mosaicados em uma única imagem, isto impossibilita o append em um Mosaic Dataset
    )
    return images

def classify_image(image: Image = None, ml_model: BaseImageClassifier = None):
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

def detect_changes(current, historic):
    aprint(message='Detectando Mudanças Imagens', progress=True)
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

def add_image_to_mosaic_dataset(image: Image, location: Database, mosaic_dataset: MosaicDataset, satelite: str = 'Sentinel2'):
    output_name = f'{satelite}_{image.date_created.strftime("%Y%m%d")}'
    new_image = Image(
        path=image.copy_image(
            delete_source=False,
            destination=location,
            output_name=output_name
        ),
        stretch_image=False
    )
    mosaic_dataset.add_images(images=new_image)
    mosaic_dataset.footprints_layer.update_rows(
        where_clause=f"Name='{output_name}'",
        fields=['DataImagem','DataProcessamento'],
        values=[image.date_created, image.date_processed]
    )

def main():

    images = get_images()

    if hasattr(VARIABLES, 'output_mosaic_dataset_current') and VARIABLES.output_mosaic_dataset_current:
        add_image_to_mosaic_dataset(
            image=images.current_image,
            location=BASE_CONFIGS.image_storage,
            mosaic_dataset=VARIABLES.output_mosaic_dataset_current
        )
    if hasattr(VARIABLES, 'output_mosaic_dataset_historic') and VARIABLES.output_mosaic_dataset_historic:
        add_image_to_mosaic_dataset(
            image=images.historic_image,
            location=BASE_CONFIGS.image_storage,
            mosaic_dataset=VARIABLES.output_mosaic_dataset_historic
        )

    current_classification = classify_image(image=images.current_image, ml_model=images.service.ml_model)
    historic_classification = classify_image(image=images.historic_image, ml_model=images.service.ml_model)

    change_detection = detect_changes(current=current_classification, historic=historic_classification)
    
    AppendResults(variables=VARIABLES, configs=BASE_CONFIGS).append_data()


if __name__ == '__main__':
    aprint(
        message=f'''
            \n1. Arquivos temporários serão salvos em: {BASE_CONFIGS.temp_dir}
            \n2. GeoDatabase temporário: {BASE_CONFIGS.temp_db.full_path}
            \n3. Arquivos baixados serão salvos em: {BASE_CONFIGS.download_storage}
            \n4. Imagens finais serão salvas em: {BASE_CONFIGS.image_storage}
            \nIniciando Execução...
            \n_ _________________ _\n'''
    )
    main()

    BASE_CONFIGS.delete_temporary_content()