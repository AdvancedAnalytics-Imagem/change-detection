import json
import logging
import os
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

    def get_images_metadata(self, area, initial_date, final_date, cloud_cover=__DEFAULT_CLOUD_COVER):
        logging.debug(f'Coordenadas da área: {area}\n'
                      f'Cobertura de nuvens: {cloud_cover}\n'
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
                    "query": {"cloud_cover": {"lte": cloud_cover}}
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