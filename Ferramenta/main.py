# -*- coding: utf-8 -*-
#!/usr/bin/python
import datetime
from datetime import datetime

from core._logs import *
from core.adapters.SateliteImagery import ImageAcquisition
from core.configs.Configs import Configs
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.instances.Images import Image
from core.instances.MosaicDataset import MosaicDataset
from core.libs.BaseProperties import BaseProperties
from core.ml_models.ImageClassifier import BaseImageClassifier

VARIABLES = Configs()
BASE_CONFIGS = BaseProperties()

def get_images():
    aprint(message='Iniciando Aquisição de Imagens', progress=True)
    images = ImageAcquisition(
        service=VARIABLES.sensor,
        credentials=VARIABLES.sentinel_api_auth,
        downloads_folder=VARIABLES.download_storage
    )
    images.get_images(
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
            destination=location.full_path,
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

    if VARIABLES.download_images:
        images = get_images()

    add_image_to_mosaic_dataset(
        image=images.current_image,
        location=VARIABLES.output_images_location,
        mosaic_dataset=VARIABLES.output_mosaic_dataset_current
    )
    add_image_to_mosaic_dataset(
        image=images.historic_image,
        location=VARIABLES.output_images_location,
        mosaic_dataset=VARIABLES.output_mosaic_dataset_historic
    )

    if VARIABLES.classify_images:
        current_classification = classify_image(image=images.current_image, ml_model=images.service.ml_model)
        historic_classification = classify_image(image=images.historic_image, ml_model=images.service.ml_model)

    if current_classification and historic_classification:
        change_detection = detect_changes(current=current_classification, historic=historic_classification)

    if VARIABLES.insert_on_database:
        aprint(message='Exportando resultados', progress=True)

        VARIABLES.output_mosaic_dataset_current.add_images(images.current_image)
        VARIABLES.output_mosaic_dataset_historic.add_images(images.historic_image)
        
        tile_names = ', '.join(images.service.tile_names)
        VARIABLES.classificacao_atual.append_dataset(origin=current_classification, extra_constant_values={
            'DATA':images.current_image.date_created,
            'DATA_PROC':images.current_image.date_processed,
            'SENSOR':VARIABLES.sensor,
            'TILES':tile_names
        })
        VARIABLES.classificacao_historica.append_dataset(origin=historic_classification, extra_constant_values={
            'DATA':images.historic_image.date_created,
            'DATA_PROC':images.historic_image.date_created,
            'SENSOR':VARIABLES.sensor,
            'TILES':tile_names
        })
        VARIABLES.deteccao_de_mudancas.append_dataset(origin=change_detection, extra_constant_values={
            'DATA_A':images.current_image.date_created,
            'DATA_H':images.historic_image.date_created,
            'DATA_PROC':images.current_image.date_processed,
            'SENSOR':VARIABLES.sensor,
            'TILES':tile_names
        })

if __name__ == '__main__':
    aprint(
        message=f'''1. Arquivos temporários serão salvos em: {BASE_CONFIGS.temp_dir}
            \n2. GeoDatabase temporário: {BASE_CONFIGS.temp_db.full_path}
            \n3. Arquivos baixados serão salvos em: {BASE_CONFIGS.download_storage}
            \n4. Imagens finais serão salvas em: {BASE_CONFIGS.image_storage}''',
        level=LogLevels.WARNING
    )
    main()
