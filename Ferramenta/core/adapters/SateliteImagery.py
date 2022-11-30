# -*- coding: utf-8 -*-
#!/usr/bin/python
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from enum import Enum, unique

from arcpy import Exists
from core._constants import *
from core._logs import *
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.instances.Images import Image
from core.libs.BaseProperties import BaseProperties
from core.libs.CustomExceptions import NoAvailableImageOnPeriod
from core.services.SateliteImagery.ImageryServices import (
    BaseImageAcquisitionService, Cbers, Sentinel2)


class ImageAcquisition(BaseProperties):
    current_image: Image
    historic_image: Image
    service: BaseImageAcquisitionService

    @unique
    class Services(Enum):
        SENTINEL2 = Sentinel2
        CBERS = Cbers

    def __init__(self, service: Services, credentials: list = []) -> None:        
        self.intersecting_tiles = []
        self.service = self.Services[service].value()

        if credentials:
            self.service.authenticate_api(credentials=credentials)

    def set_downloaded_images_path(self, *args, **kwargs) -> None:
        self.service.set_downloaded_images_path(*args, **kwargs)

    def get_historic_and_current_images(
        self,
        area_of_interest: Feature,
        results_output_location: Database = None,
        max_cloud_coverage: int = None,
        compose_as_single_image: bool = True,
        days_period: int = None,
        max_date: datetime = None) -> None:
        """Busca as imagens históricas e atuais, baixa e cria um mosaico dos diferentes tiles
            Args:
                area_of_interest (Feature): _description_
                results_output_location (Database, optional): _description_. Defaults to None.
                max_cloud_coverage (int, optional): _description_. Defaults to None.
                compose_as_single_image (bool, optional): _description_. Defaults to True.
        """
        if not max_date:
            max_date = self.now
        if not days_period:
            days_period = self.service._days_gap
        aprint(f' > Buscando Imagens Histórica e Atual\n    - Sensor: CBERS - Data máxima: {max_date} - Período: {days_period} dias')

        #* Current Image acquisition
        current_image_name = f'Atual_{self.today_str}'

        self.current_image = self.get_composed_images_for_aoi(
            max_date=max_date,
            area_of_interest=area_of_interest,
            results_output_location=results_output_location,
            max_cloud_coverage=max_cloud_coverage,
            compose_as_single_image=compose_as_single_image,
            output_img_name=current_image_name,
            days_period=days_period
        )

        #* Historic Image acquisition
        historic_image_name = f'Hist_{self.today_str}'
        min_search_date = max_date - timedelta(days=days_period)
        
        self.historic_image = self.get_composed_images_for_aoi(
            max_date=min_search_date,
            area_of_interest=area_of_interest,
            results_output_location=results_output_location,
            max_cloud_coverage=max_cloud_coverage,
            compose_as_single_image=compose_as_single_image,
            output_img_name=historic_image_name,
            days_period=days_period
        )

    def get_composed_images_for_aoi(
        self,
        max_date: datetime,
        area_of_interest: Feature,
        days_period: int = None,
        results_output_location: Database = None,
        max_cloud_coverage: int = None,
        compose_as_single_image: bool = True,
        output_img_name: str = ''
    ) -> Image:
        if not results_output_location:
            results_output_location = self.temp_db

        if max_cloud_coverage:
            self.service.max_cloud_coverage = max_cloud_coverage
        
        if not self.intersecting_tiles:
            self.intersecting_tiles = self.service.get_selected_tiles_names(area_of_interest=area_of_interest)
        self.progress_tracker.init_tracking(total=len(self.intersecting_tiles), name='Busca por Imagens')

        #* Image acquisition
        if not output_img_name:
            output_img_name = f'Img{self.today_str}'

        resulting_image = os.path.join(results_output_location.full_path, f'Stch_Msk_Mos_{output_img_name}')
        if Exists(resulting_image):
            image = Image(path=resulting_image, stretch_image=False)
            image.date_created = max_date
            return image

        self.service.query_available_images(
            area_of_interest=area_of_interest,
            max_date=max_date,
            days_period=days_period
        )

        images = {}
        futures = []

        # with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
        #     for tile in self.intersecting_tiles:
        #         futures.append(
        #             executor.submit(
        #                 self.service.get_best_available_images_for_tile,
        #                 tile
        #             )
        #         )

        #     for response in as_completed(futures):
        #         result = response.result()
        #         tile_images = result.get('images')
        #         tile = result.get('tile')
        #         if tile_images:
        #             for tile_image in tile_images:
        #                 images[tile_image.datetime] = [*images.get(tile_image.datetime,[]), tile_image]
        #         self.progress_tracker.report_progress(add_progress=True)

        for tile in self.intersecting_tiles:
            result = self.service.get_best_available_images_for_tile(tile_name=tile, area_of_interest=area_of_interest)
            tile_images = result.get('images')
            tile = result.get('tile')
            if tile_images:
                for tile_image in tile_images:
                    images[tile_image.datetime] = [*images.get(tile_image.datetime,[]), tile_image]
            self.progress_tracker.report_progress(add_progress=True)

        composition_images = []
        [composition_images.extend(i) for i in images.values()]

        if not composition_images:
            raise NoAvailableImageOnPeriod(
                tiles=self.service.tile_names,
                period=self.service._days_gap
            )

        # Composed image with all available downloaded images for that tile
        image = Image(
            path=results_output_location.full_path,
            name=output_img_name,
            images_for_composition=composition_images,
            compose_as_single_image=compose_as_single_image,
            mask=area_of_interest
        )

        tiles_dates = list(images.keys())
        tiles_dates.sort()
        image.date_created = tiles_dates[-1]
        return image
