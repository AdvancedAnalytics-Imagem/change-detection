# -*- coding: utf-8 -*-
#!/usr/bin/python
from datetime import date, datetime, timedelta
from enum import Enum, unique

from arcpy import Exists
from core._constants import *
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.instances.Images import Image
from core.libs.BaseProperties import BaseProperties
from core.services.SateliteImagery.ImageryServices import Cbers, Sentinel2, BaseImageAcquisitionService


class ImageAcquisition(BaseProperties):
    current_image: Image
    historic_image: Image
    _date_gap: int = 30

    @unique
    class Services(Enum):
        SENTINEL2 = Sentinel2
        CBERS = Cbers

    def __init__(self, service: Services, credentials: list = [], downloads_folder: str = None) -> None:        
        self.intersecting_tiles = []
        self.service = self.Services[service].value(downloads_folder=downloads_folder)
        if credentials:
            self.service.authenticate_api(credentials=credentials)

    def set_downloaded_images_path(self, *args, **kwargs) -> None:
        self.service.set_downloaded_images_path(*args, **kwargs)

    def get_images(self, area_of_interest: Feature, results_output_location: Database = None, max_cloud_coverage: int = None, compose_as_single_image: bool = True):
        """Busca as imagens históricas e atuais, baixa e cria um mosaico dos diferentes tiles
            Args:
                area_of_interest (Feature): _description_
                results_output_location (Database, optional): _description_. Defaults to None.
                max_cloud_coverage (int, optional): _description_. Defaults to None.
                compose_as_single_image (bool, optional): _description_. Defaults to True.
        """

        #* Current Image acquisition
        current_image_name = f'Current_Image_{self.today_str}'

        self.current_image = self.get_image(
            max_date=self.now,
            days_period=self._date_gap,
            area_of_interest=area_of_interest,
            results_output_location=results_output_location,
            max_cloud_coverage=max_cloud_coverage,
            compose_as_single_image=compose_as_single_image,
            output_img_name=current_image_name
        )

        #* Historic Image acquisition
        historic_image_name = f'Historic_Image_{self.today_str}'
        min_search_date = self.current_image.date_created - timedelta(days=self._date_gap)
        
        self.historic_image = self.get_image(
            max_date=min_search_date,
            days_period=self._date_gap,
            area_of_interest=area_of_interest,
            results_output_location=results_output_location,
            max_cloud_coverage=max_cloud_coverage,
            compose_as_single_image=compose_as_single_image,
            output_img_name=historic_image_name
        )

    def get_image(self, max_date: datetime, days_period: int, area_of_interest: Feature, results_output_location: Database = None, max_cloud_coverage: int = None, compose_as_single_image: bool = True, output_img_name: str = ''):
        if not results_output_location:
            results_output_location = self.temp_db

        if max_cloud_coverage:
            self.service.max_cloud_coverage = max_cloud_coverage
        
        if not self.intersecting_tiles:
            self.intersecting_tiles = self.service.get_selected_tiles_names(area_of_interest=area_of_interest)
        self.progress_tracker.init_tracking(total=len(self.intersecting_tiles), name='Busca por Imagens')

        #* Image acquisition
        images = {}
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

        for tile in self.intersecting_tiles:
            tile_images = self.service.get_image(tile_name=tile, area_of_interest=area_of_interest)
            for tile_image in tile_images:
                images[tile_image.datetime] = [*images.get(tile_image.datetime,[]), tile_image]
            self.progress_tracker.report_progress(add_progress=True)

        composition_images = []
        [composition_images.extend(i) for i in images.values()]

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
