# -*- coding: utf-8 -*-
#!/usr/bin/python
import os
from dataclasses import dataclass
from datetime import date, datetime
from xml.etree import ElementTree as ET

from arcpy import Exists, EnvManager
from arcpy.management import CompositeBands, Delete, MosaicToNewRaster, CopyRaster
from arcpy.sa import Stretch, ExtractByMask
from arcpy.ia import ClassifyPixelsUsingDeepLearning
from core._constants import *
from core.instances.Database import Database, wrap_on_database_editing
from core.libs.Base import (BaseConfig, BasePath, load_path_and_name,
                                   prevent_server_error)
from sentinelsat import (SentinelAPI, geojson_to_wkt, make_path_filter,
                         read_geojson)
from sentinelsat.exceptions import ServerError as SetinelServerError

from .Feature import Feature


class SentinelImage(BaseConfig, BasePath):
    title: str = None
    geometry: dict = None
    datetime: datetime = None
    date: date = None
    tileid: str = None
    properties: dict = None,
    uuid: str = ''
    id: str = ''
    nodata_pixel_percentage_str: str = ''
    relativeorbitnumber: int = 0
    cloud_coverage: float = 0.0

    def __init__(self, api: any, *args, **kwargs):
        self.api = api
        self.__dict__.update(kwargs)
        self.nodata_pixel_percentage_str = ''
        self.split_title_data()

    def get(self, property: str):
        try:
            return self.properties.get(property)
        except Exception as e:
            print(e)
            return None

    @property
    def nodata_pixel_percentage(self):
        if not self.nodata_pixel_percentage_str:
            self.fetch_s2_qi_info()
        if self.nodata_pixel_percentage_str:
            try:
                return float(self.nodata_pixel_percentage_str)
            except Exception as e:
                print(f'Não foi possível converter o valor de nodata {self.nodata_pixel_percentage_str} para decimal (float).\n{e}')
        return 100

    def split_title_data(self):
        title_parts = self.title.split('_')
        self.tileid = title_parts[5][1:]
        self.datetime = datetime.strptime(title_parts[6], format('%Y%m%dT%H%M%S'))
        self.date = self.datetime.date()

    # ---- Funções para buscar informações do nodata_pixel_percentage ----
    def get_odata_file_url(self, path: str) -> str:
        odata_path = f"{self.api.api_url}odata/v1/Products('{self.uuid}')"
        for p in path.split("/"):
            odata_path += f"/Nodes('{p}')"
        odata_path += "/$value"
        return odata_path

    @prevent_server_error
    def fetch_s2_qi_info(self) -> None:
        if self.api.is_online(self.uuid):
            path = f"{self.title}.SAFE/MTD_MSIL2A.xml"
            url = self.get_odata_file_url(path)
            response = self.api.session.get(url)
            if '504 Gateway Time-out' in str(response.content):
                raise SetinelServerError
            if not response.content:
                self.nodata_pixel_percentage_str = '100'
            else:
                xml = ET.XML(response.content)
                for elem in xml.find(".//Image_Content_QI"):
                    if "." in elem.text and elem.tag == "NODATA_PIXEL_PERCENTAGE":
                        self.nodata_pixel_percentage_str = elem.text
    # ---- Funções para buscar informações do nodata_pixel_percentage ----
    
    @prevent_server_error
    def download(self, filter: list[str], downloads_folder: str) -> dict:
        return self.api.download(self.uuid, directory_path=downloads_folder, checksum=False, nodefilter=make_path_filter(filter))

    def download_image(self, image_database: Database, downloads_folder: str, output_name: str = '', delete_temp_files: bool = False):
        print(f'Baixando bandas da cena {self.uuid}')
        filterB2 = "*_B02_10m*"
        filterB3 = "*_B03_10m*"
        filterB4 = "*_B04_10m*"
        filterB8 = "*_B08_10m*"
        filterList = [filterB2, filterB3, filterB4, filterB8]
        
        downloaded_images = []
        for filter in filterList:
            try:
                downloaded_image = self.download(filter=filter, downloads_folder=downloads_folder)
            except Exception as e:
                print(f'Erro ao baixar imagem.\n{e}')
                continue
            downloaded_images.append(downloaded_image)
        if not downloaded_images: return
        self.unzip_files(folder=downloads_folder)

        images_folder = os.path.join(downloads_folder, os.path.basename(downloaded_image.get('node_path')))

        image_bands = self.get_files_by_extension(folder=images_folder)
        if not output_name:
            output_name = os.path.basename(downloaded_image.get('title'))
        try:
            self.full_path = os.path.join(image_database.full_path, output_name)
            if not Exists(self.full_path):
                print(f'Composing bands - {output_name}')
                CompositeBands(";".join(image_bands), self.full_path)
        except Exception as e:
            print(e)

        if delete_temp_files:
            try:
                print(f"""  APAGANDO IMAGENS INTERMEDIÁRIAS""")
                Delete(images_folder)
                print(f"""  APAGADO COM SUCESSO!""")
            except Exception as e:
                print(e)

class Image(BasePath, BaseConfig):
    _masked_prefix: str = 'Msk_'
    _mosaic_prefix: str = 'Mos_'
    _stretch_prefix: str = 'Stch_'
    _copy_prefix: str = 'Copy_'
    _classification_prefix: str = 'Clssif_'
    temp_destination: str = 'IN_MEMORY'
    processor_type: str = 'CPU'
    processing_date: datetime = datetime.now()
    date_created: datetime = datetime.now()
    database: Database = None
    base_images: list = []

    def __init__(self, path: str, name: str = None, images_for_composition: list[SentinelImage] = [], mask: Feature = None, temp_destination: str or Database = None, *args, **kwargs):
        if temp_destination:
            if not isinstance(temp_destination, Database):
                temp_destination = Database(temp_database)
            self.temp_destination = temp_destination
        
        super(Image, self).__init__(path=path, name=name, *args, **kwargs)
        self.database = Database(path=path, create=True)
        self.base_images = images_for_composition
        if images_for_composition:
            self.mosaic_images(images_for_composition=images_for_composition)
        if mask and isinstance(mask, Feature):
            self.extract_by_mask(area_of_interest=mask)

    @property
    def is_inside_database(self):
        return self.database is not None
        
    @wrap_on_database_editing
    def mosaic_images(self, images_for_composition: list[str]) -> str:
        list_of_images_paths = []
        for image in images_for_composition:
            if not isinstance(image, str):
                if hasattr(image, 'full_path'):
                    path = image.full_path
            else:
                path = image

            if Exists(path):
                list_of_images_paths.append(path)

        self.name = f'{self._mosaic_prefix}{self.name}'
        if not Exists(os.path.join(self.database.full_path, self.name)):
            print(f'Criando Mosaico em {self.database.full_path}')
            MosaicToNewRaster(
                input_rasters=list_of_images_paths,
                output_location=self.database.full_path,
                raster_dataset_name_with_extension=self.name,
                number_of_bands=4,
                pixel_type='16_BIT_UNSIGNED',
                cellsize=10,
                mosaic_method='MAXIMUM',
                mosaic_colormap_mode='MATCH'
            )

        self.full_path = os.path.join(self.database.full_path, self.name)
        return self.full_path

    @wrap_on_database_editing
    def extract_by_mask(self, area_of_interest: Feature) -> str:
        self.name = f'{self._masked_prefix}{self.name}'
        self.path = self.database.full_path
        if not Exists(os.path.join(self.path, self.name)):
            print(f'Extraindo máscara da imagem {self.full_path}')
            clipped_mosaic = ExtractByMask(
                in_raster=self.full_path,
                in_mask_data=area_of_interest.full_path
            )
            self.full_path = os.path.join(self.path, self.name)
            clipped_mosaic.save(self.full_path)
        else:
            self.full_path = os.path.join(self.path, self.name)

        return self.full_path
    
    @wrap_on_database_editing
    def stretch_image(self) -> str:
        self.name = f'{self._stretch_prefix}{self.name}'
        self.path = self.database.full_path
        if not Exists(os.path.join(self.path, self.name)):
            print(f'Aplicando Strech na Imagem {self.full_path}')
            stretch = Stretch(
                raster=self.full_path,
                stretch_type="StdDev",
                min=0,
                max=255,
                num_stddev=None,
                statistics=None,
                dra=False,
                min_percent=0.25,
                max_percent=0.75,
                gamma=None,
                compute_gamma=False,
                sigmoid_strength_level=None
            )
            self.full_path = os.path.join(self.path, self.name)
            stretch.save(self.full_path)
        else:
            self.full_path = os.path.join(self.path, self.name)
        
        return self.full_path

    def copy_image(self, pixel_type: str = None, nodata_value: str = '', background_value: float = None, destination: str or Database = None) -> str:
        """Creates a copy of the current image
            Args:
                destination (str, optional): path to the folder to receive the raster. Defaults to 'IN_MEMORY'.
                pixel_type (str, optional): If none, the raster pixel type will be used. Defaults to None.
                > 1_BIT | 2_BIT | 4_BIT | 8_BIT_UNSIGNED | 8_BIT_SIGNED | 16_BIT_UNSIGNED | 16_BIT_SIGNED | 32_BIT_UNSIGNED | 32_BIT_SIGNED | 32_BIT_FLOAT | 64_BIT
            Returns:
                str: Path to the new copy
        """
        if not destination:
            destination = self.temp_destination
        elif not isinstance(destination, Database):
            destination = Database(path=destination)

        destination.start_editing()
        result = f'{self._copy_prefix}{self.name}'
        print(f'Criando cópia de {self.full_path}')
        CopyRaster(
            in_raster=self.full_path,
            out_rasterdataset=result,
            config_keyword='',
            background_value=background_value,
            nodata_value=nodata_value,
            onebit_to_eightbit="NONE",
            colormap_to_RGB="NONE",
            pixel_type=pixel_type,
            scale_pixel_value="NONE",
            RGB_to_Colormap="NONE",
            format="GRID",
            transform="NONE",
            process_as_multidimensional="CURRENT_SLICE",
            build_multidimensional_transpose="NO_TRANSPOSE"
        )
        destination.close_editing()
        return os.path.join(destination.full_path, result)

    def get_image_nodata_area(self, output_path: str or Database = None):
        # Creates a black and white copy
        copy = self.copy_image(pixel_type='1_BIT', destination=output_path)
        return Feature(path=f'{copy}_polygon', raster=copy, temp_destination=self.temp_destination)

    def classify(self, classifier: str, output_path: Database):
        print(f'Classificando a imagem {self.full_path}')
        classified_raster_full_path = os.path.join(self.temp_destination.full_path, f'{self._classification_prefix}{self.name}')
        if not Exists(classified_raster_full_path):
            self.temp_destination.start_editing()
            with EnvManager(processorType=self.processor_type):
                try:
                    arguments="padding 70;batch_size 2;predict_background True;tile_size 256"
                    out_classified_raster = ClassifyPixelsUsingDeepLearning(
                        in_raster=self.full_path,
                        in_model_definition=classifier,
                        arguments=arguments,
                        processing_mode="PROCESS_AS_MOSAICKED_IMAGE",
                        out_classified_folder=None
                    )
                    out_classified_raster.save(classified_raster_full_path)
                except Exception as e:
                    raise e
            self.temp_destination.close_editing()
        self.processing_date = self.now
        feature = Feature(path=f'{classified_raster_full_path}_polygon', raster=classified_raster_full_path, temp_destination=self.temp_destination)
        return feature

