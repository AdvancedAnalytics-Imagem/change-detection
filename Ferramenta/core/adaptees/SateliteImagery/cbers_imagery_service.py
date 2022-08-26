import glob
import json
import logging
import os
import urllib
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor

import arcpy
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
    __PRJ = 'PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Mercator_Auxiliary_Sphere"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],PARAMETER["Standard_Parallel_1",0.0],PARAMETER["Auxiliary_Sphere_Type",0.0],UNIT["Meter",1.0]]'

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.tiles_layer = Feature(path=self.base_gbd.full_path, name=self.__tiles_layer_name)
        self.images_database = Database(path=os.path.dirname(self.images_folder), name='CBERS_IMAGES')

    def create_mosaic(self, download_folder: str):
        print(f'Iniciando mosaico das imagens')
        base_images = []
        for file in os.listdir(download_folder):
            if '_pansharp.tif' in file:
                base_images.append(f"{download_folder}\\{file}")
        arcpy.management.MosaicToNewRaster(';'.join(base_images), download_folder, "MOS_A.tif",
                                           CBERSImageryService.__PRJ, "16_BIT_UNSIGNED", 2, 4, "MEAN", "MATCH")
        self.__delete_temp_files(download_folder)
        print(f'Finalizado mosaico das imagens')

    def compose_image(self, files: {}, download_folder: str):
        for file_id in files.keys():
            print(f'Iniciando composição da imagem {file_id}')
            compose_img = f"{download_folder}\\{file_id}_compose.tif"
            self.__delete_temp_files(compose_img, delete_base=True)
            filepaths = [
                files[file_id]['nir_img'],
                files[file_id]['red_img'],
                files[file_id]['green_img'],
                files[file_id]['blue_img']
            ]
            arcpy.management.CompositeBands(';'.join(filepaths), compose_img)
            self.__delete_temp_files(compose_img)
            print(f'Finalizado composição da imagem {file_id}')
            self.__pansharp_image(file_id, download_folder, files[file_id]['pan_img'])

    def __delete_temp_files(self, filepath: str, delete_base=False):
        extensions = ['.tfw', '.tif.aux.xml', '.tif.ovr', '.tif.xml']
        for ext in extensions:
            tmp_file = filepath[0:-4] + ext
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
        if delete_base:
            if os.path.exists(filepath):
                os.remove(filepath)


    def __pansharp_image(self, file_id: str, download_folder: str, pan_img: str):
        print(f'Iniciando pansharpening da imagem {file_id}')
        compose_img = f"{download_folder}\\{file_id}_compose.tif"
        pansharp_img = f"{download_folder}\\{file_id}_pansharp.tif"
        self.__delete_temp_files(pansharp_img, delete_base=True)
        arcpy.CreatePansharpenedRasterDataset_management(
            compose_img, '1', '2', '3', '4', pansharp_img, pan_img, 'Gram-Schmidt'
        )
        self.__delete_temp_files(pansharp_img)
        print(f'Finalizado pansharpening da imagem {file_id}')

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
        files = {}
        for index, row in metadata.iterrows():
            files[row['id']] = {
                'pan_img': f"{download_folder}\\p_{row['id']}.tif",
                'red_img': f"{download_folder}\\r_{row['id']}.tif",
                'green_img': f"{download_folder}\\g_{row['id']}.tif",
                'blue_img': f"{download_folder}\\b_{row['id']}.tif",
                'nir_img': f"{download_folder}\\n_{row['id']}.tif"
            }
        with ThreadPoolExecutor(max_workers=5) as threads:
            print('Iniciando downloads')
            thread_list = []
            for file_data in files.values():
                thread_list.append(threads.submit(self.__download_worker, row['id'], row['pan_url'], file_data['pan_img']))
                thread_list.append(threads.submit(self.__download_worker, row['id'], row['red_url'], file_data['red_img']))
                thread_list.append(threads.submit(self.__download_worker, row['id'], row['green_url'], file_data['green_img']))
                thread_list.append(threads.submit(self.__download_worker, row['id'], row['blue_url'], file_data['blue_img']))
                thread_list.append(threads.submit(self.__download_worker, row['id'], row['nir_url'], file_data['nir_img']))
                futures.wait(thread_list)
                for thread in thread_list:
                    if thread.exception() is not None:
                        self.__erase_image(row['id'])
                        raise Exception(f'Falha ao baixar a imagem.', str(thread.exception()))
            print('Downloads finalizados')
            return files

    def __download_worker(self, id: str, url: str, filepath: str) -> None:
        if os.path.exists(filepath):
            logging.debug(f'Arquivo de imagem {filepath} já existe')
            print(f'Arquivo de imagem {filepath} já existe')
        else:
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