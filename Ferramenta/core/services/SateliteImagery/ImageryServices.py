# -*- coding: utf-8 -*-
#!/usr/bin/python
import json
import os
from collections import OrderedDict
from datetime import date, datetime, timedelta
from http import HTTPStatus

import requests
from arcpy.da import SearchCursor
from core._constants import *
from core._logs import *
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.instances.Images import CbersImage, SentinelImage
from core.libs.Base import prevent_server_error
from core.libs.BaseProperties import BaseProperties
from core.libs.CustomExceptions import (NoBaseTilesLayerFound,
                                        NoCbersCredentials,
                                        NoImageFoundForTile,
                                        PansharpCustomException)
from core.ml_models.ImageClassifier import (BaseImageClassifier,
                                            CbersImageClassifier,
                                            Sentinel2ImageClassifier)
from nbformat import ValidationError
from sentinelsat import SentinelAPI, geojson_to_wkt


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
        self.selected_tiles = Feature(
            path=self.tiles_layer.select_by_location(
                intersecting_feature=area_of_interest,
                in_memory=True
            )
        )
        return self.selected_tiles

    def query_available_images(self, *args, **kwargs) -> dict:
        pass

    def get_best_available_images_for_tile(
        self,
        tile_name:str,
        area_of_interest: Feature = None,
        max_date: datetime = None,
        days_period: int = None,
        image_prefix: str = None
    ) -> dict:
        if not image_prefix:
            image_prefix = f'{self.sensor[:2]}{self.sensor[-1]}'
            
        if not self.available_images:
            if not area_of_interest:
                raise ValidationError('Não existem imagens em memória, para busca-las é necessário informar uma area de interesse')
            self.query_available_images(area_of_interest=area_of_interest, max_date=max_date, days_period=days_period)

        available_tile_images = self.available_images.get(tile_name)
        if not available_tile_images:
            NoImageFoundForTile(tile_name)
            return {}
            
        best_available_images = self._get_most_recent_image(
            images=available_tile_images,
            max_date=max_date,
            days_period=days_period)
        
        if not best_available_images:
            NoImageFoundForTile(tile_name)
            return {}

        if not isinstance(best_available_images, list):
            best_available_images = [best_available_images]

        [image.download_image(
            image_database=self.images_database,
            output_name=f'{image_prefix}_{tile_name}'
        ) for image in best_available_images]

        # List of best images Instances (already downloaded)
        return {'images':best_available_images, 'tile':tile_name}
    
    @staticmethod
    def _sort_images_by_date(images) -> list:
        images_dict = {i.date:i for i in images}
        images_dict_keys = list(images_dict.keys())
        images_dict_keys.sort(reverse=True)
        return [images_dict.get(key) for key in images_dict_keys]
    
    def _get_most_recent_image(self, images: list, max_date: datetime = None, days_period: datetime = None) -> SentinelImage:
        if not images: return
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

    def get_selected_tiles_names(self, area_of_interest: Feature = None, where_clause: str = None, name_field: str = 'NAME') -> list:
        if not self.selected_tiles:
            self._select_tiles(area_of_interest=area_of_interest, where_clause=where_clause)
        self.tile_names = [i[0] for i in SearchCursor(self.selected_tiles.full_path, [name_field])]
        aprint(f'      > Tiles sendo processados: | {" | ".join(self.tile_names)} |')
        return self.tile_names
    
    
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

    def get_selected_tiles_names(self, *args, **kwargs) -> list:
        return super().get_selected_tiles_names(name_field='PATH_ROW', *args, **kwargs)
        
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

    def get_best_available_images_for_tile(
        self,
        tile_name: str,
        area_of_interest: Feature = None,
        max_date: datetime = None,
        days_period: int = None,
        image_prefix: str = None
    ) -> dict:
        if not image_prefix:
            image_prefix = f'{self.sensor[:2]}{self.sensor[-1]}'
            
        if not self.available_images:
            if not area_of_interest:
                raise ValidationError('Não existem imagens em memória, para busca-las é necessário informar uma area de interesse')
            self.query_available_images(area_of_interest=area_of_interest, max_date=max_date, days_period=days_period)

        available_tile_images = self.available_images.get(tile_name)
        if not available_tile_images:
            NoImageFoundForTile(tile_name)
            return {}
            
        best_available_images = self._sort_images_by_date(images=available_tile_images)
        
        if not best_available_images:
            NoImageFoundForTile(tile_name)
            return {}

        if not isinstance(best_available_images, list):
            best_available_images = [best_available_images]

        downloaded_images = []
        for image in best_available_images:
            try:
                image.download_image(
                    image_database=self.images_database,
                    output_name=f'{image_prefix}_{tile_name}'
                )
                downloaded_images.append(image)
                break
            except PansharpCustomException:
                continue

        # List of best images Instances (already downloaded)
        return {'images':downloaded_images, 'tile':tile_name}
    
    def query_available_images(self, area_of_interest: Feature, max_date: datetime, days_period: int):
        if not max_date: max_date = self.today
        if not days_period: days_period = self._days_gap
        begin_date = max_date - timedelta(days=days_period)
        end_date = max_date + timedelta(days=1)
        area = area_of_interest.bounding_box()
        
        aprint(f'      > Buscando imagens Disponíveis > CBERS - {begin_date.date()} a {end_date.date()}')
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

    def get_selected_tiles_names(self, *args, **kwargs) -> list:
        return super().get_selected_tiles_names(name_field='NAME', *args, **kwargs)

    def query_available_images(self, area_of_interest: Feature, max_date: datetime, days_period: int) -> dict:
        if not max_date: max_date = self.today
        if not days_period: days_period = self._days_gap
        begin_date = max_date - timedelta(days=days_period)
        end_date = max_date + timedelta(days=1)
        self.min_date = begin_date
        self.max_date = end_date
        aoi_geojson = area_of_interest.geojson_geometry()
        area = geojson_to_wkt(aoi_geojson)
        
        aprint(f'   > Sentinel2 - {begin_date.date()} a {end_date.date()}')
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

    def get_best_available_images_for_tile(self, tile_name:str, area_of_interest: Feature = None, max_date: datetime = None, days_period: int = None, image_prefix: str = None) -> dict:
        if not image_prefix:
            image_prefix = f'{self.sensor[:2]}{self.sensor[-1]}'
            
        if not self.available_images:
            if not area_of_interest:
                raise ValidationError('Não existem imagens em memória, para busca-las é necessário informar uma area de interesse')
            self.query_available_images(area_of_interest=area_of_interest, max_date=max_date, days_period=days_period)

        available_tile_images = self.available_images.get(tile_name)
        if not available_tile_images:
            NoImageFoundForTile(tile_name)
            return {}
            
        best_available_images = self._get_best_possile_images_based_on_coverage(
            images=available_tile_images
        )
        
        if not best_available_images:
            NoImageFoundForTile(tile_name)
            return {}

        if not isinstance(best_available_images, list):
            best_available_images = [best_available_images]

        [image.download_image(
            image_database=self.images_database,
            output_name=f'{image_prefix}_{tile_name}'
        ) for image in best_available_images]

        # List of best images Instances (already downloaded)
        return {'images':best_available_images, 'tile':tile_name}
    
    def _get_best_possile_images_based_on_coverage(self, images: list, min_coverage: int = 98, combined_coverage: int = 170) -> list:
        response = []
        coverage = 0
        list_of_images_ordered_by_date = self._sort_images_by_date(images)
        for image in list_of_images_ordered_by_date:
            image_coverage = 100 - image.nodata_pixel_percentage

            if image_coverage < 2: # Cobertura baixa demais para considerar a imagem
                continue
            
            if image_coverage > min_coverage: # Cobertura alta o suficiente para não precisar de outra imagem
                return image
            
            response.append(image)
            coverage += image_coverage
            if coverage > combined_coverage: # Múltiplas images que recobrem o tile em 170% provavelmente compensam o no_data
                return response
        
        return response
