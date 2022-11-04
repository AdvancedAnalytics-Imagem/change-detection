# -*- coding: utf-8 -*-
#!/usr/bin/python
import json
import os
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta
from http import HTTPStatus

import requests
from arcpy.da import SearchCursor
from core._constants import *
from core._logs import *
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.instances.Images import (BaseSateliteImage, CbersImage, Image,
                                   SentinelImage)
from core.libs.Base import ProgressTracker, prevent_server_error
from core.libs.BaseProperties import BaseProperties
from core.libs.CustomExceptions import NoBaseTilesLayerFound, NoCbersCredentials
from core.ml_models.ImageClassifier import (BaseImageClassifier,
                                            CbersImageClassifier,
                                            Sentinel2ImageClassifier)
from nbformat import ValidationError
from sentinelsat import (SentinelAPI, geojson_to_wkt, make_path_filter,
                         read_geojson)


class BaseImageAcquisitionService(BaseProperties):
    gdb_name = 'ServicesSupportData.gdb'
    images_folder: str = None
    max_cloud_coverage: int = 20
    _tiles_layer_name: str = ''
    _days_gap: int = 30
    selected_tiles: any = None
    tiles_layer: Feature = None

    def __init__(self, *args, **kwargs) -> None:
        super(BaseImageAcquisitionService, self).__init__(*args, **kwargs)

        self.base_gbd = Database(path=IMAGERY_SERVICES_DIR, name=self.gdb_name)
        self.set_downloaded_images_path(path=self.download_storage)
        
        self.tiles_layer = Feature(path=self.base_gbd.full_path, name=self._tiles_layer_name)
        self.images_database = Database(path=os.path.dirname(self.images_folder), name='COMPOSED_IMAGES')
    
    def set_downloaded_images_path(self, path: str) -> None:
        if path and not path.endswith('Downloaded_Images'): path = os.path.join(path, 'Downloaded_Images')
        self.images_folder = self.load_path_variable(path=path)
    
    def authenticate_api(self, *args, **kwargs) -> None:
        pass
    
    @property
    def ml_model(self) -> BaseImageClassifier:
        pass

    def _select_tiles(self, area_of_interest: Feature, where_clause: str = None) -> Feature:
        if where_clause:
            area_of_interest.select_by_attributes(where_clause=where_clause)
        if not self.tiles_layer:
            raise NoBaseTilesLayerFound()
        self.selected_tiles = Feature(path=self.tiles_layer.select_by_location(intersecting_feature=area_of_interest))
        return self.selected_tiles

    def query_available_images(self, *args, **kwargs) -> dict:
        pass

    def get_best_available_images_for_tile(self, *args, **kwargs) -> list:
        pass
    
    def _get_most_recent_image(self, images: list, max_date: datetime = None, days_period: datetime = None) -> SentinelImage:
        most_recent_image = None
        min_date = None
        if max_date:
            if not days_period: days_period = self._days_gap
            min_date = max_date - timedelta(days=days_period)

        for image in images:
            if (min_date and image.datetime < min_date) or (max_date and image.datetime >= max_date) or (most_recent_image and image.datetime <= most_recent_image.datetime):
                continue # Image date is not between min_date and max_date or is older then the current selected image
            most_recent_image = image
        return most_recent_image


class Cbers(BaseImageAcquisitionService):
    _collections = [
        # {"name": "CBERS4A_WFI_L4_DN"},
        # {"name": "CBERS4A_WFI_L2_DN"},
        # {"name": "CBERS4A_MUX_L4_DN"},
        # {"name": "CBERS4A_MUX_L2_DN"},
        {"name": "CBERS4A_WPM_L4_DN"},
        {"name": "CBERS4A_WPM_L2_DN"},
    ]
    _tiles_layer_name = 'grade_cebers_brasil'
    _days_gap: int = 60
    credentials: dict = {}

    @property
    def ml_model(self) -> BaseImageClassifier:
        return CbersImageClassifier()

    def get_selected_tiles_names(self, area_of_interest: Feature = None, where_clause: str = None) -> list:
        if not self.selected_tiles:
            self._select_tiles(area_of_interest=area_of_interest, where_clause=where_clause)
        self.tile_names = [i[0] for i in SearchCursor(self.selected_tiles.full_path, ['PATH_ROW'])]
        aprint(f'Tiles selecionados:\n{",".join(self.tile_names)}')
        return self.tile_names
        
    @prevent_server_error
    def _query_images(self, area: str, begin_date: datetime, end_date: datetime) -> list:
        payload = json.dumps(
            {
                "providers": [
                    {
                        "name": "INPE-CDSR",
                        "collections": self._collections,
                        "method": "POST",
                        "query": {"cloud_cover": {"lte": self.max_cloud_coverage}}
                    }
                ],
                "bbox": area,
                "datetime": f"{self.format_date_as_str(begin_date)}/{self.format_date_as_str(end_date)}",
                "limit": 10000
            }
        )
        headers = {'Content-Type': 'application/json'}
        response = requests.post(self.credentials.get('url'), headers=headers, data=payload)
        scenes = []

        if response.status_code != HTTPStatus.OK.value:
            aprint(f'Não foram encontradas imagens que se enquadrem nos parâmetros.\n{response}')
            return []
        
        feature_collection = response.json().get('INPE-CDSR',{})
        feature_list = []
        for collection in feature_collection:
            feature_list.extend(feature_collection.get(collection).get('features',[]))
        for scene in feature_list:
            scene['properties']['col'] = scene['properties'].pop('path')
            scene['properties']['cloudcoverpercentage'] = scene['properties'].pop('cloud_cover')
            scenes.append(
                {
                    'id': scene.get('id'),
                    'properties': scene.get('properties'),
                    'pan_url': f"{scene.get('assets')['pan']['href']}?email={self.credentials.get('user')}",
                    'red_url': f"{scene.get('assets')['red']['href']}?email={self.credentials.get('user')}",
                    'blue_url': f"{scene.get('assets')['blue']['href']}?email={self.credentials.get('user')}",
                    'green_url': f"{scene.get('assets')['green']['href']}?email={self.credentials.get('user')}",
                    'nir_url': f"{scene.get('assets')['nir']['href']}?email={self.credentials.get('user')}"
                }
            )
        return scenes

    def query_available_images(self, area_of_interest: Feature, max_date: datetime, days_period: int):
        if not max_date: max_date = self.today
        if not days_period: days_period = self._days_gap
        begin_date = max_date - timedelta(days=days_period)
        end_date = max_date + timedelta(days=1)
        area = area_of_interest.bounding_box()
        
        aprint(f'> Buscando imagens do sensor CBERS entre {begin_date.date()} e {end_date.date()}')
        identified_images = self._query_images(area=area, begin_date=begin_date, end_date=end_date)
        self.available_images = {}
        for image_feature in identified_images:
            image_properties = image_feature.get('properties',{})
            image_title = image_feature.get('id', False)
            if not image_title: continue

            cbers_image = CbersImage(
                geometry=image_feature.get('geometry',{}),
                properties=image_properties,
                title=image_title,
                pan_url=image_feature.get('pan_url'),
                red_url=image_feature.get('red_url'),
                blue_url=image_feature.get('blue_url'),
                green_url=image_feature.get('green_url'),
                nir_url=image_feature.get('nir_url'),
                **image_properties
            )
            self.available_images[cbers_image.tileid] = [*self.available_images.get(cbers_image.tileid,[]), cbers_image]

        return self.available_images

    def authenticate_api(self, credentials: list) -> None:
        self.credentials = credentials.get('cbers_api',{})
        if not self.credentials:
            raise NoCbersCredentials()
    
    def _get_best_possile_images(self, list_of_images: list, max_date: datetime = None, days_period: datetime = None) -> list:
        return self._get_most_recent_image(images=list_of_images, max_date=max_date, days_period=days_period)

    def get_best_available_images_for_tile(self, tile_name:str, area_of_interest: Feature = None, max_date: datetime = None, days_period: int = None) -> list:
        if not self.available_images:
            if not area_of_interest:
                raise ValidationError('Não existem imagens em memória, para busca-las é necessário informar uma area de interesse')
            self.query_available_images(area_of_interest=area_of_interest, max_date=max_date, days_period=days_period)

        best_available_image = self._get_best_possile_images(
            list_of_images=self.available_images.get(tile_name,[]),
            max_date=max_date,
            days_period=days_period)

        if not best_available_image:
            aprint(f'Não foi possível encontrar imagem disponível para o tile {tile_name} no período de interesse.', level=LogLevels.WARNING)
            return []

        if not isinstance(best_available_image, list): best_available_image = [best_available_image]
        [image.download_image(
            image_database=self.images_database,
            output_name=f'CBR_{tile_name}'
        ) for image in best_available_image]

        # List of best images Instances (already downloaded)
        return best_available_image

class Sentinel2(BaseImageAcquisitionService):
    _scene_min_coverage_threshold: float = 1.5
    _combined_scene_min_coverage_threshold: float = 90
    _tiles_layer_name: str = 'grade_sentinel_brasil'
    available_images: dict = {}
    apis: list = []

    def authenticate_api(self, credentials: list) -> None:
        credentials = credentials.get('sentinel2_api')
        if not isinstance(credentials, list):
            credentials = [credentials]
        for credential in credentials:
            self.apis.append(SentinelAPI(*list(credential.values())))

    @property    
    def ml_model(self) -> BaseImageClassifier:
        return Sentinel2ImageClassifier()
    
    @prevent_server_error
    def _query_images(self, api: any, area: str, begin_date: datetime, end_date: datetime) -> list:
        if not api: return []
        # payload = {
        #     "area": area,
        #     "producttype": "SLC",
        #     "platformname": "Sentinel-1",
        #     "date": (begin_date, end_date)
        # }
        payload = {
            "area": area,
            "area_relation": "Intersects",
            "producttype": "S2MSI2A",
            "platformname": "Sentinel-2",
            "cloudcoverpercentage": (0, self.max_cloud_coverage),
            "raw": f"filename:S2*",
            "date": (begin_date, end_date)
        }
        products = OrderedDict()
        products.update(api.query(**payload))
        return api.to_geojson(products).features

    def get_selected_tiles_names(self, area_of_interest: Feature = None, where_clause: str = None) -> list:
        if not self.selected_tiles:
            self._select_tiles(area_of_interest=area_of_interest, where_clause=where_clause)
        self.tile_names = [i[0] for i in SearchCursor(self.selected_tiles.full_path, ['NAME'])]
        aprint(f'Tiles selecionados:\n{",".join(self.tile_names)}')
        return self.tile_names

    def query_available_images(self, area_of_interest: Feature, max_date: datetime, days_period: int) -> dict:
        if not max_date: max_date = self.today
        if not days_period: days_period = self._days_gap
        begin_date = max_date - timedelta(days=days_period)
        end_date = max_date + timedelta(days=1)
        self.min_date = begin_date
        self.max_date = end_date
        aoi_geojson = area_of_interest.geojson_geometry()
        area = geojson_to_wkt(aoi_geojson)
        
        aprint(f'> Buscando imagens do sensor Sentinel2 entre {begin_date.date()} e {end_date.date()}')
        # Sentinel has multiple APIs, so this loops through it and pre loads credentials
        for api in self.apis:
            identified_images = self._query_images(api=api, area=area, begin_date=begin_date, end_date=end_date)
            self.available_images = {}
            for image_feature in identified_images:
                image_properties = image_feature.get('properties',{})
                image_title = image_properties.get('title', False)
                if not image_title: continue

                sentinel_image = SentinelImage(
                    api=api,
                    geometry=image_feature.get('geometry',{}),
                    properties=image_properties,
                    **image_properties
                )
                self.available_images[sentinel_image.tileid] = [*self.available_images.get(sentinel_image.tileid,[]), sentinel_image]

        return self.available_images

    def _get_best_possile_images(self, list_of_images: list, max_date: datetime = None, days_period: datetime = None) -> list:
        """Looks throught the identified images on the selected period for the current tile and isolates the best and most recent image based on a few rules
            Args:
                list_of_images (list): List of images identified for the current tile
                tile (str): Tile name
                max_date: Max date for an image (Optional)
                days_period: Min date for an image (Optional)
            Returns:
                Image -> Most recent available Image instance,
        """
        response = []
        coverage = 0
        list_of_images_ordered_by_date = self.order_images_by_date(list_of_images)
        for image in list_of_images_ordered_by_date:
            image_coverage = 100 - image.nodata_pixel_percentage
            if image_coverage > 98.5:
                return image
            
            response.append(image)
            coverage += image_coverage
            if coverage > 170:
                return response
        
        return response
        # return self._combine_lower_coverage_tile_image(images=list_of_images, max_date=max_date, days_period=days_period)

    def order_images_by_date(self, list_of_images: list) -> list[Image]:
        images = {}
        for image in list_of_images:
            images[image.date] = image
        dates = list(images.keys())
        dates.sort()
        return [images.get(k) for k in dates]
    
    def _combine_lower_coverage_tile_image(self, images: list, max_date: datetime = None, days_period: datetime = None) -> list:
        filtered_list_of_images = self._filter_by_cloud_coverage(images=images, threshold=self.max_cloud_coverage)

        if not filtered_list_of_images or len(filtered_list_of_images) < 2: return filtered_list_of_images

        first_image = self._get_most_recent_image(images=filtered_list_of_images, max_date=max_date, days_period=days_period)
        second_image = self._get_most_recent_image(images=filtered_list_of_images, max_date=first_image.datetime, days_period=days_period)
        return [first_image, second_image]

    @staticmethod
    def _filter_by_cloud_coverage(images: list, threshold: int) -> list:
        return [image for image in images if image.cloud_coverage < threshold]

    @staticmethod
    def _filter_by_nodata_threshold(images: list, threshold: int) -> list:
        if not images: return []
        return [image for image in images if image.nodata_pixel_percentage < threshold]

    def get_best_available_images_for_tile(self, tile_name:str, area_of_interest: Feature = None, max_date: datetime = None, days_period: int = None) -> list:
        if not self.available_images:
            if not area_of_interest:
                raise ValidationError('Não existem imagens em memória, para busca-las é necessário informar uma area de interesse')
            self.query_available_images(area_of_interest=area_of_interest, max_date=max_date, days_period=days_period)

        available_tile_images = self.available_images.get(tile_name)
        best_available_image = self._get_best_possile_images(
            list_of_images=available_tile_images,
            max_date=max_date,
            days_period=days_period
        )

        if not best_available_image:
            aprint(f'Não existe imagem disponível para o tile {tile_name}.', level=LogLevels.ERROR)
            return []

        if not isinstance(best_available_image, list): best_available_image = [best_available_image]
        [image.download_image(
            image_database=self.images_database,
            output_name=f'SNT2_{tile_name}'
        ) for image in best_available_image]

        # List of best images Instances (already downloaded)
        return {'images':best_available_image, 'tile':tile_name}
