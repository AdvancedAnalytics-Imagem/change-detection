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
from core.services.SateliteImagery.ImageryServices import Cbers, Sentinel2


class ImageAcquisition(BaseProperties):
    current_image: Image
    historic_image: Image

    @unique
    class Services(Enum):
        SENTINEL2 = Sentinel2
        CBERS = Cbers

    def __init__(self, service: str = 'SENTINEL2', credentials: list = [], downloads_folder: str = None) -> None:        
        self.service = self.Services[service].value(downloads_folder=downloads_folder)
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
        if max_cloud_coverage:
            self.service.max_cloud_coverage = max_cloud_coverage

        intersecting_tiles = self.service.get_selected_tiles_names(area_of_interest=area_of_interest)
        self.progress_tracker.init_tracking(total=len(intersecting_tiles)*2, name='Busca por Imagens')

        #* Current Image acquisition

        current_images = {}
        current_image_name = f'Current_Image_{self.today_str}'

        if not Exists(
            os.path.join(results_output_location.full_path, f'Stch_Msk_Mos_{current_image_name}')
            ):
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
                name=current_image_name,
                images_for_composition=composition_images,
                compose_as_single_image=compose_as_single_image,
                mask=area_of_interest
            )

            tiles_dates = list(current_images.keys())
            tiles_dates.sort()
            tiles_min_date = tiles_dates[0]
            tiles_max_date = tiles_dates[-1]

        else:
            self.current_image = Image(
                path=results_output_location.full_path,
                name=f'Stch_Msk_Mos_{current_image_name}',
                stretch_image=False
            )
            tiles_max_date = self.today

        self.current_image.date_created = tiles_max_date

        #* Historic Image acquisition

        historic_images = {}
        historic_image_name = f'Historic_Image_{self.today_str}'
        min_search_date = tiles_max_date - timedelta(days=30)

        if not Exists(os.path.join(results_output_location.full_path, f'Stch_Msk_Mos_{historic_image_name}')):
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
                name=historic_image_name,
                images_for_composition=hist_composition_images,
                compose_as_single_image=compose_as_single_image,
                mask=area_of_interest
            )
            hist_tiles_dates = list(historic_images.keys())
            hist_tiles_dates.sort()
            hist_tiles_max_date = hist_tiles_dates[-1]
        else:
            self.historic_image = Image(
                path=results_output_location.full_path,
                name=f'Stch_Msk_Mos_{historic_image_name}',
                stretch_image=False
            )
            hist_tiles_max_date = min_search_date

        self.historic_image.date_created = hist_tiles_max_date
