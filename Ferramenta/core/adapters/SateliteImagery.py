# -*- coding: utf-8 -*-
#!/usr/bin/python
from datetime import date, datetime, timedelta
from enum import Enum, unique

from core._constants import *
from core.adaptees.SateliteImagery.ImageryServices import Cebers, Sentinel2
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.instances.Images import Image
from core.libs.Base import BaseConfig


class ImageAcquisition(BaseConfig):
    current_image: Image
    historic_image: Image
    temp_destination: str or Database = 'IN_MEMORY'

    @unique
    class Services(Enum):
        SENTINEL2 = Sentinel2
        CYBERS = Cebers

    def __init__(self, service: str = 'SENTINEL2', credentials: list = [], downloads_folder: str = None, temp_destination: str or Database = None) -> None:
        if temp_destination:
            if not isinstance(temp_destination, Database):
                temp_destination = Database(temp_database)
            self.temp_destination = temp_destination
        
        self.service = self.Services[service].value(downloads_folder=downloads_folder)
        self.service.authenticate_api(credentials=credentials)

    def set_downloaded_images_path(self, *args, **kwargs) -> None:
        self.service.set_downloaded_images_path(*args, **kwargs)

    def get_images(self, area_of_interest: Feature, results_output_location: Database = None, max_cloud_coverage: int = None, compose_as_single_image: bool = True):
        if max_cloud_coverage:
            self.service.max_cloud_coverage = max_cloud_coverage

        intersecting_tiles = self.service.get_selected_tiles_names(area_of_interest=area_of_interest)
        self.progress_tracker.init_tracking(total=len(intersecting_tiles)*2, name='Busca por Imagens')

        #* Current Image acquisition

        current_images = {}
        self.service.query_available_images(area_of_interest=area_of_interest)

        for tile in intersecting_tiles:
            tile_images = self.service.get_image(tile_name=tile, area_of_interest=area_of_interest)
            for tile_image in tile_images:
                current_images[tile_image.datetime] = [*current_images.get(tile_image.datetime,[]), tile_image]
            self.progress_tracker.report_progress(add_progress=True)

        composition_images = []
        [composition_images.extend(i) for i in current_images.values()]

        self.current_image = Image(
            path=results_output_location.full_path,
            name=f'Current_Image_{self.format_date_as_str(current_date=self.now, return_format="%Y%m%d")}',
            images_for_composition=composition_images,
            compose_as_single_image=compose_as_single_image,
            mask=area_of_interest,
            temp_destination=self.temp_destination
        )

        tiles_dates = list(current_images.keys())
        tiles_dates.sort()
        tiles_min_date = tiles_dates[0]
        tiles_max_date = tiles_dates[-1]

        self.current_image.date_created = tiles_max_date

        #* Historic Image acquisition

        historic_images = {}
        min_search_date = tiles_min_date - timedelta(days=30)
        self.service.query_available_images(area_of_interest=area_of_interest, max_date=min_search_date)

        for tile in intersecting_tiles:
            tile_images = self.service.get_image(tile_name=tile, area_of_interest=area_of_interest, max_date=min_search_date)
            for tile_image in tile_images:
                historic_images[tile_image.datetime] = [*historic_images.get(tile_image.datetime,[]), tile_image]
            self.progress_tracker.report_progress(add_progress=True)
        
        hist_composition_images = []
        [hist_composition_images.extend(i) for i in historic_images.values()]

        self.historic_image = Image(
            path=results_output_location.full_path,
            name=f'Historic_Image_{self.format_date_as_str(current_date=self.now, return_format="%Y%m%d")}',
            images_for_composition=hist_composition_images,
            compose_as_single_image=compose_as_single_image,
            mask=area_of_interest,
            temp_destination=self.temp_destination
        )

        hist_tiles_dates = list(historic_images.keys())
        hist_tiles_dates.sort()
        hist_tiles_max_date = hist_tiles_dates[-1]

        self.historic_image.date_created = hist_tiles_max_date
