# -*- coding: utf-8 -*-
#!/usr/bin/python
import os
import time
from collections import OrderedDict
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import urllib
import json

import pandas as pd
import requests
from arcpy.da import SearchCursor
from core._constants import *
from core._logs import *
from core.instances.Database import Database
from core.instances.Feature import Feature
from core.instances.Images import Image, SentinelImage
from core.libs.Base import (BaseConfig, BasePath, ProgressTracker,
                            prevent_server_error)
from core.ml_models.ImageClassifier import Sentinel2ImageClassifier, BaseImageClassifier
from nbformat import ValidationError
from sentinelsat import (SentinelAPI, geojson_to_wkt, make_path_filter,
                         read_geojson)


class BaseImageAcquisitionService(BasePath, BaseConfig):
    gdb_name = 'ServicesSupportData.gdb'
    images_folder: str = None

    def __init__(self, downloads_folder: str = None, *args, **kwargs) -> None:
        if not downloads_folder: downloads_folder = DOWNLOADS_DIR
        super(BaseImageAcquisitionService, self).__init__(*args, **kwargs)

        self.base_gbd = Database(path=IMAGERY_SERVICES_DIR, name=self.gdb_name)
        self.set_downloaded_images_path(path=downloads_folder)
    
    def set_downloaded_images_path(self, path: str) -> None:
        if path and not path.endswith('Downloaded_Images'): path = os.path.join(path, 'Downloaded_Images')
        self.images_folder = self.load_path_variable(path=path)
        aprint(f'Image Downloads Folder: {self.images_folder}', level=LogLevels.INFO)

    
    def authenticate_api(self, *args, **kwargs) -> None:
        pass
    
    @property    
    def ml_model(self) -> BaseImageClassifier:
        pass

    def get_selected_tiles_names(self, *args, **kwargs) -> list:
        pass

    def query_available_images(self, *args, **kwargs) -> dict:
        pass

    def get_image(self, *args, **kwargs) -> list:
        pass

class Sentinel2(BaseImageAcquisitionService):
    _scene_min_coverage_threshold: float = 1.5
    _combined_scene_min_coverage_threshold: float = 90
    _tiles_layer_name: str = 'grade_sentinel_brasil'
    _query_days_before_today: int = 30
    max_cloud_coverage: int = 20
    selected_tyles: any = None
    available_images: dict = {}
    apis: list = []
    
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.tiles_layer = Feature(path=self.base_gbd.full_path, name=self._tiles_layer_name)
        self.images_database = Database(path=os.path.dirname(self.images_folder), name='SENTINEL_IMAGES')

    def authenticate_api(self, credentials: list) -> None:
        if not isinstance(credentials, list):
            credentials = [credentials]
        for credential in credentials:
            self.apis.append(SentinelAPI(*list(credential.values())))

    @property    
    def ml_model(self):
        return Sentinel2ImageClassifier()

    def _select_tiles(self, area_of_interest: Feature, where_clause: str = None) -> Feature:
        if where_clause:
            area_of_interest.select_by_attributes(where_clause=where_clause)
        self.selected_tyles = Feature(path=self.tiles_layer.select_by_location(intersecting_feature=area_of_interest))
        return self.selected_tyles
    
    def get_selected_tiles_names(self, area_of_interest: Feature = None, where_clause: str = None) -> list:
        if not self.selected_tyles:
            self._select_tiles(area_of_interest=area_of_interest, where_clause=where_clause)
        self.tile_names = [i[0] for i in SearchCursor(self.selected_tyles.full_path, ['NAME'])]
        aprint(f'Tiles selecionados:\n{",".join(self.tile_names)}')
        return self.tile_names
    
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

    def query_available_images(self, area_of_interest: Feature, max_date: datetime = None) -> dict:
        if not max_date: max_date = self.today
        begin_date = max_date - timedelta(days=self._query_days_before_today)
        end_date = max_date + timedelta(days=1)
        aoi_geojson = area_of_interest.geojson_geometry()
        area = geojson_to_wkt(aoi_geojson)
        aprint(f'Buscando por imagens disponíveis entre {begin_date} e {end_date} na área de interesse')
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

    def _get_best_possile_images(self, list_of_images: list, tile_name: str, max_date: datetime = None, min_date: datetime = None) -> list:
        """Looks throught the identified images on the selected period for the current tile and isolates the best and most recent image based on a few rules
            Args:
                list_of_images (list): List of images identified for the current tile
                tile (str): Tile name
                max_date: Max date for an image (Optional)
                min_date: Min date for an image (Optional)
            Returns:
                Image -> Most recent available Image instance,
        """
        filtered_list_of_images = self._filter_by_nodata_threshold(images=list_of_images, threshold=self._scene_min_coverage_threshold)
        if filtered_list_of_images:
            filtered_list_of_images = self._filter_by_cloud_coverage(images=filtered_list_of_images, threshold=self.max_cloud_coverage)
            return self._get_most_recent_image(images=filtered_list_of_images, max_date=max_date, min_date=min_date)

        return self._combine_lower_coverage_tile_image(images=list_of_images, max_date=max_date, min_date=min_date)

    def _combine_lower_coverage_tile_image(self, images: list, max_date: datetime = None, min_date: datetime = None) -> list:
        filtered_list_of_images = self._filter_by_nodata_threshold(images=images, threshold=self._combined_scene_min_coverage_threshold)
        filtered_list_of_images = self._filter_by_cloud_coverage(images=filtered_list_of_images, threshold=self.max_cloud_coverage)

        if not filtered_list_of_images or len(filtered_list_of_images) < 2: return

        first_image = self._get_most_recent_image(images=filtered_list_of_images, max_date=max_date, min_date=min_date)
        second_image = self._get_most_recent_image(images=filtered_list_of_images, max_date=first_image.datetime, min_date=min_date)
        return [first_image, second_image]

    @staticmethod
    def _filter_by_cloud_coverage(images: list, threshold: int) -> list:
        return [image for image in images if image.cloud_coverage < threshold]

    @staticmethod
    def _filter_by_nodata_threshold(images: list, threshold: int) -> list:
        if not images: return []
        return [image for image in images if image.nodata_pixel_percentage < threshold]
    
    @staticmethod
    def _get_most_recent_image(images: list, max_date: datetime = None, min_date: datetime = None) -> SentinelImage:
        most_recent_image = None
        for image in images:
            if (min_date and image.datetime < min_date) or (max_date and image.datetime >= max_date) or (most_recent_image and image.datetime <= most_recent_image.datetime):
                continue # Image date is not between min_date and max_date or is older then the current selected image
            most_recent_image = image
        return most_recent_image

    def get_image(self, tile_name:str, area_of_interest: Feature = None, max_date: datetime = None, min_date: datetime = None) -> list:
        if not self.available_images:
            if not self.area_of_interest:
                raise ValidationError('Não existem imagens em memória, para busca-las é necessário informar uma area de interesse')
            self.query_available_images(area_of_interest=area_of_interest)

        available_tile_images = self.available_images.get(tile_name)
        best_available_image = self._get_best_possile_images(list_of_images=available_tile_images, tile_name=tile_name)

        if not best_available_image:
            aprint(f'Não existe imagem disponível para o tile {tile_name} no intervalo dos últimos {self._query_days_before_today} que se enquadre nos parâmetros de filtro especificados')
            return []

        if not isinstance(best_available_image, list): best_available_image = [best_available_image]
        [image.download_image(
            image_database=self.images_database,
            downloads_folder=self.images_folder,
            output_name=f'ComposedTile_{tile_name}'
        ) for image in best_available_image]

        return best_available_image


class Cebers(BaseImageAcquisitionService):
    __LYR_CBERS = "CBERS 4A"
    __LYR_GRID_CBERS = None
    __DEFAULT_CLOUD_COVER = 50
    __CLEAR_OUTPUT_IMAGES = 180
    __CBERS_URL = 'http://www2.dgi.inpe.br/stac-compose/stac/search/'
    __CBERS_USER = 'caesb_testes@outlook.com'
    __CBER_SENSOR = 'WPM'
    __CBER_OUTPUT = ''
    __tiles_layer_name: str = 'grade_cbers_brasil'

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.tiles_layer = Feature(path=self.base_gbd.full_path, name=self.__tiles_layer_name)
        self.images_database = Database(path=os.path.dirname(self.images_folder), name='CBERS_IMAGES')

    def __get_images_metadata(self, area: [], initial_date: datetime, final_date: datetime) -> pd.DataFrame:
        logging.debug(f'Coordenadas da área: {area}\n'
                      f'Cobertura de nuvens: {Cebers.__DEFAULT_CLOUD_COVER}\n'
                      f'Data inicial: {initial_date.strftime("%d/%m/%Y")}\n'
                      f'Data final: {final_date.strftime("%d/%m/%Y")}')
        payload = json.dumps({
            "providers": [
                {
                    "name": "INPE-CDSR",
                    "collections": [
                        {"name": "CBERS4A_WPM_L4_DN"},
                        {"name": "CBERS4A_WPM_L2_DN"},
                        {"name": "CBERS4A_WFI_L4_DN"},
                        {"name": "CBERS4A_WFI_L2_DN"},
                        {"name": "CBERS4A_MUX_L4_DN"},
                        {"name": "CBERS4A_MUX_L2_DN"}
                    ],
                    "method": "POST",
                    "query": {"cloud_cover": {"lte": Cebers.__DEFAULT_CLOUD_COVER}}
                }
            ],
            "bbox": area,
            "datetime": f"{datetime.strftime(initial_date, '%Y-%m-%d')}T00:00:00/{datetime.strftime(final_date, '%Y-%m-%d')}T23:59:00",
            "limit": 10000
        })
        headers = {'Content-Type': 'application/json'}
        response = requests.request('POST', Cebers.__CBERS_URL, headers=headers, data=payload)
        scenes = []
        if Cebers.__CBER_SENSOR == 'WPM':
            features = json.loads(response.text)
            feature_list = features['INPE-CDSR']['CBERS4A_WPM_L4_DN']['features'] + \
                           features['INPE-CDSR']['CBERS4A_WPM_L2_DN']['features']
            for scene in feature_list:
                scenes.append({
                    'id': scene['id'],
                    'datetime': scene['properties']['datetime'],
                    'cloudcover': scene['properties']['cloud_cover'],
                    'path': scene['properties']['path'],
                    'row': scene['properties']['row'],
                    'pan_url': f"{scene['assets']['pan']['href']}?email={Cebers.__CBERS_USER}",
                    'red_url': f"{scene['assets']['red']['href']}?email={Cebers.__CBERS_USER}",
                    'blue_url': f"{scene['assets']['blue']['href']}?email={Cebers.__CBERS_USER}",
                    'green_url': f"{scene['assets']['green']['href']}?email={Cebers.__CBERS_USER}",
                    'nir_url': f"{scene['assets']['nir']['href']}?email={Cebers.__CBERS_USER}"
                })
            return pd.DataFrame(scenes).sort_values(by=['cloudcover', 'datetime'], ascending=[True, False])

    def download_images(self, area: [], initial_date: datetime, final_date: datetime, download_folder: str) -> None:
        metadata = self.__get_images_metadata(area, initial_date, final_date)
        with ThreadPoolExecutor(max_workers=5) as threads:
            print('Iniciando downloads')
            for index, row in metadata.iterrows():
                pan_thread = threads.submit(self.__download_worker, row['id'], row['pan_url'], download_folder, 'pan')
                red_thread = threads.submit(self.__download_worker, row['id'], row['red_url'], download_folder, 'red')
                green_thread = threads.submit(self.__download_worker, row['id'], row['green_url'], download_folder,
                                              'green')
                blue_thread = threads.submit(self.__download_worker, row['id'], row['blue_url'], download_folder,
                                             'blue')
                nir_thread = threads.submit(self.__download_worker, row['id'], row['nir_url'], download_folder, 'nir')
                futures.wait([pan_thread, red_thread, green_thread, blue_thread, nir_thread])
                if pan_thread.exception() is not None:
                    self.__erase_image(row['id'])
                    raise Exception(f'Falha ao baixar a imagem PAN', str(pan_thread.exception()))
                if red_thread.exception() is not None:
                    self.__erase_image(row['id'])
                    raise Exception(f'Falha ao baixar a imagem RED', str(red_thread.exception()))
                if green_thread.exception() is not None:
                    self.__erase_image(row['id'])
                    raise Exception(f'Falha ao baixar a imagem GREEN', str(green_thread.exception()))
                if blue_thread.exception() is not None:
                    self.__erase_image(row['id'])
                    raise Exception(f'Falha ao baixar a imagem BLUE', str(blue_thread.exception()))
                if nir_thread.exception() is not None:
                    self.__erase_image(row['id'])
                    raise Exception(f'Falha ao baixar a imagem NIR', str(nir_thread.exception()))
            print('Downloads finalizados')

    def __download_worker(self, id: str, url: str, folder: str, image_type: str) -> None:
        filepath = f"{folder}\\{id}.tif"
        if image_type == 'pan':
            filepath = f"{folder}\\p_{id}.tif"
        elif image_type == 'red':
            filepath = f"{folder}\\r_{id}.tif"
        elif image_type == 'green':
            filepath = f"{folder}\\g_{id}.tif"
        elif image_type == 'blue':
            filepath = f"{folder}\\b_{id}.tif"
        elif image_type == 'nir':
            filepath = f"{folder}\\n_{id}.tif"
        logging.debug(f'Baixando imagem {id} => {filepath}')
        print(f'Baixando imagem {id} => {filepath}')
        urllib.request.urlretrieve(url, filepath)

    def __erase_image(self, folder: str, id: str) -> None:
        try:
            for prefix in ['p_', 'r_', 'g_', 'b_', 'n_']:
                filepath = f"{folder}\\{prefix}{id}.tif"
                if os.path.exists(filepath):
                    os.remove(filepath)
        except:
            pass
