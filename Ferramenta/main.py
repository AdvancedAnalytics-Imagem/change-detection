# -*- coding: utf-8 -*-
#!/usr/bin/python
# interpreter > C:\Users\Matheus Caputo Pires\AppData\Local\ESRI\conda\envs\arcgispro-py3-deeplearning
import datetime
from datetime import datetime

from core.adapters.SateliteImagery import ImageAcquisition
from core.configs.Configs import Configs
from core.instances.Feature import Feature
from core.instances.Images import Image


def main():
    variables = Configs()

    if variables.download_images:
        satelite_images_service = ImageAcquisition(
            service='SENTINEL2',
            credentials=variables.sentinel_api_auth,
            downloads_folder=variables.downloads_storage,
            temp_destination=variables.temp_db
        )
        satelite_images_service.get_images(
            area_of_interest=variables.target_area,
            results_output_location=variables.output_images_location,
            max_cloud_coverage=variables.max_cloud_coverage
        )

        if variables.classify_images:
            current_classification = satelite_images_service.current_image.classify(
                classifier=satelite_images_service.service.ml_model,
                output_path=variables.temp_db
            )

            historic_classification = satelite_images_service.historic_image.classify(
                classifier=satelite_images_service.service.ml_model,
                output_path=variables.temp_db
            )

            change_detection = Feature(path=historic_classification.intersects(intersecting_feature=current_classification))

            if variables.insert_on_database:
                variables.classificacao_atual.append_dataset(origin=current_classification, extra_constant_values={
                    'DATA_A':satelite_images_service.current_image.date_created
                })
                variables.classificacao_historica.append_dataset(origin=historic_classification, extra_constant_values={
                    'DATA_H':satelite_images_service.historic_image.date_created
                })
                variables.deteccao_de_mudancas.append_dataset(origin=change_detection, extra_constant_values={
                    'DATA_A':satelite_images_service.current_image.date_created,
                    'DATA_H':satelite_images_service.historic_image.date_created,
                    'DATA_PROC':datetime.datetime.now()
                })

if __name__ == '__main__':
    main()
