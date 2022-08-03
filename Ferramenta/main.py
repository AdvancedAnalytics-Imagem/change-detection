# -*- coding: utf-8 -*-
#!/usr/bin/python
# interpreter > C:\Users\Matheus Caputo Pires\AppData\Local\ESRI\conda\envs\arcgispro-py3-deeplearning
import datetime
from datetime import datetime

from core.adapters.SateliteImagery import ImageAcquisition
from core.configs.Configs import Configs
from core.instances.Feature import Feature
from core.instances.Images import Image

variables = Configs()

def get_images():
    images = ImageAcquisition(
        service=variables.sensor,
        credentials=variables.sentinel_api_auth,
        downloads_folder=variables.downloads_storage,
        temp_destination=variables.temp_db
    )
    images.get_images(
        area_of_interest=variables.target_area,
        results_output_location=variables.output_images_location,
        max_cloud_coverage=variables.max_cloud_coverage
    )

    return images


def classify_image(image = None, ml_model = None):
    if not image or not ml_model: return
    classification = image.classify(
        classifier=ml_model,
        output_path=variables.temp_db
    )
    return classification

def detect_changes(current, historic):
    change_detection = Feature(path=historic.intersects(intersecting_feature=current))
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
    
def main():

    if variables.download_images:
        images = get_images()

    if variables.classify_images:
        current_classification = classify_image(image=images.current_image, ml_model=images.service.ml_model)
        historic_classification = classify_image(image=images.historic_image, ml_model=images.service.ml_model)

    if current_classification and historic_classification:
        change_detection = detect_changes(current=current_classification, historic=historic_classification)

    if variables.insert_on_database:
        variables.classificacao_atual.append_dataset(origin=current_classification, extra_constant_values={
            'DATA':images.current_image.date_created,
            'DATA_PROC':datetime.datetime.now(),
            'SENSOR':variables.sensor,
            'TILES':', '.join(images.tile_names)
        })
        variables.classificacao_historica.append_dataset(origin=historic_classification, extra_constant_values={
            'DATA':images.historic_image.date_created,
            'DATA_PROC':datetime.datetime.now(),
            'SENSOR':variables.sensor,
            'TILES':', '.join(images.tile_names)
        })
        variables.deteccao_de_mudancas.append_dataset(origin=change_detection, extra_constant_values={
            'DATA_A':images.current_image.date_created,
            'DATA_H':images.historic_image.date_created,
            'DATA_PROC':datetime.datetime.now(),
            'SENSOR':variables.sensor,
            'TILES':', '.join(images.tile_names)
        })

if __name__ == '__main__':
    main()
