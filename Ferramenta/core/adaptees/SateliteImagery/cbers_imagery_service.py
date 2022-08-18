import json
import logging
import os
import urllib
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
import pandas as pd
import requests
from datetime import datetime
from core.adaptees.SateliteImagery.ImageryServices import BaseImageAcquisitionService
from core.instances.Database import Database
from core.instances.Feature import Feature


class CBERSImageryService(BaseImageAcquisitionService):

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
                      f'Cobertura de nuvens: {CBERSImageryService.__DEFAULT_CLOUD_COVER}\n'
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
                    "query": {"cloud_cover": {"lte": CBERSImageryService.__DEFAULT_CLOUD_COVER}}
                }
            ],
            "bbox": area,
            "datetime": f"{datetime.strftime(initial_date, '%Y-%m-%d')}T00:00:00/{datetime.strftime(final_date, '%Y-%m-%d')}T23:59:00",
            "limit": 10000
        })
        headers = {'Content-Type': 'application/json'}
        response = requests.request('POST', CBERSImageryService.__CBERS_URL, headers=headers, data=payload)
        scenes = []
        if CBERSImageryService.__CBER_SENSOR == 'WPM':
            features = json.loads(response.text)
            feature_list = features['INPE-CDSR']['CBERS4A_WPM_L4_DN']['features'] + features['INPE-CDSR']['CBERS4A_WPM_L2_DN']['features']
            for scene in feature_list:
                scenes.append({
                    'id': scene['id'],
                    'datetime': scene['properties']['datetime'],
                    'cloudcover': scene['properties']['cloud_cover'],
                    'path': scene['properties']['path'],
                    'row': scene['properties']['row'],
                    'pan_url': f"{scene['assets']['pan']['href']}?email={CBERSImageryService.__CBERS_USER}",
                    'red_url': f"{scene['assets']['red']['href']}?email={CBERSImageryService.__CBERS_USER}",
                    'blue_url': f"{scene['assets']['blue']['href']}?email={CBERSImageryService.__CBERS_USER}",
                    'green_url': f"{scene['assets']['green']['href']}?email={CBERSImageryService.__CBERS_USER}",
                    'nir_url': f"{scene['assets']['nir']['href']}?email={CBERSImageryService.__CBERS_USER}"
                })
            return pd.DataFrame(scenes).sort_values(by=['cloudcover', 'datetime'], ascending=[True, False])

    def download_images(self, area: [], initial_date: datetime, final_date: datetime, download_folder: str) -> None:
        metadata = self.__get_images_metadata(area, initial_date, final_date)
        with ThreadPoolExecutor(max_workers=5) as threads:
            print('Iniciando downloads')
            for index, row in metadata.iterrows():
                pan_thread = threads.submit(self.__download_worker, row['id'], row['pan_url'], download_folder, 'pan')
                red_thread = threads.submit(self.__download_worker, row['id'], row['red_url'], download_folder, 'red')
                green_thread = threads.submit(self.__download_worker, row['id'], row['green_url'], download_folder, 'green')
                blue_thread = threads.submit(self.__download_worker, row['id'], row['blue_url'], download_folder, 'blue')
                nir_thread = threads.submit(self.__download_worker, row['id'], row['nir_url'], download_folder, 'nir')
                futures.wait([pan_thread, red_thread, green_thread, blue_thread, nir_thread])
                if pan_thread.exception() is not None:
                    raise Exception(f'Falha ao baixar a imagem PAN', str(pan_thread.exception()))
                if red_thread.exception() is not None:
                    raise Exception(f'Falha ao baixar a imagem RED', str(red_thread.exception()))
                if green_thread.exception() is not None:
                    raise Exception(f'Falha ao baixar a imagem GREEN', str(green_thread.exception()))
                if blue_thread.exception() is not None:
                    raise Exception(f'Falha ao baixar a imagem BLUE', str(blue_thread.exception()))
                if nir_thread.exception() is not None:
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
