# -*- coding: utf-8 -*-
#!/usr/bin/python
import os
from datetime import date, datetime
from xml.etree import ElementTree as ET

from arcpy import EnvManager, Exists
from arcpy.ia import ClassifyPixelsUsingDeepLearning
from arcpy.management import (CompositeBands, CopyRaster, Delete,
                              MosaicToNewRaster)
from arcpy.sa import ExtractByMask, Stretch
from core._constants import *
from core._logs import *
from core.instances.Database import Database, wrap_on_database_editing
from core.instances.MosaicDataset import MosaicDataset
from core.libs.Base import (delete_source_files, load_path_and_name,
                            prevent_server_error)
from core.libs.BaseProperties import BaseProperties
from core.libs.BaseDBPath import BaseDBPath
from core.ml_models.ImageClassifier import BaseImageClassifier
from sentinelsat import (SentinelAPI, geojson_to_wkt, make_path_filter,
                         read_geojson)
from sentinelsat.exceptions import ServerError as SetinelServerError

from .Feature import Feature


class BaseSateliteImage(BaseProperties):
    title: str = None
    datetime: datetime = None
    date: date = None
    tileid: str = None
    properties: dict = None
    uuid: str = ''
    cloudcoverpercentage: float = 1.0

    @property
    def nodata_pixel_percentage(self, *args, **kwargs) -> float:
        """Satelite images have areas of no data availability, which is represented by this parameter
            Returns:
                float: Percentage of image coverage in decimal form -> 1.2 means 1.2 % of the image has no coverage
        """
        return 100.0
    
    @property
    def cloud_coverage(self, *args, **kwargs) -> float:
        """For images that are not Radar or Lidar based, this parameter should return the cloud coverage percentage
            Returns:
                float: Percentage -> 10.5 (Originates from cloudcoverpercentage that stores the percentage in decimal form -> 0.105)
        """
        return self.cloudcoverpercentage*100

    def download_image(self, *args, **kwargs) -> None:
        """Downloads each band on the image and composes all as one, and deletes the original download folder"""
        pass


class SentinelImage(BaseSateliteImage):
    nodata_pixel_percentage_str: str = ''

    def __init__(self, api: any, *args, **kwargs):
        self.api = api
        self.__dict__.update(kwargs)
        self.nodata_pixel_percentage_str = ''
        self._split_title_data()

    def _split_title_data(self):
        title_parts = self.title.split('_')
        self.tileid = title_parts[5][1:]
        self.datetime = datetime.strptime(title_parts[6], format('%Y%m%dT%H%M%S'))
        self.date = self.datetime.date()

    def get(self, property: str):
        return self.properties.get(property)

    # ---- Funções para buscar informações do nodata_pixel_percentage ----
    @property
    def nodata_pixel_percentage(self) -> float:
        if not self.nodata_pixel_percentage_str: self._fetch_s2_qi_info()
        if self.nodata_pixel_percentage_str:
            try:
                return float(self.nodata_pixel_percentage_str)
            except Exception as e:
                aprint(f'Não foi possível converter o valor de nodata {self.nodata_pixel_percentage_str} para decimal (float).\n{e}')
        return 100
    def _get_odata_file_url(self, path: str) -> str:
        odata_path = f"{self.api.api_url}odata/v1/Products('{self.uuid}')"
        for p in path.split("/"):
            odata_path += f"/Nodes('{p}')"
        odata_path += "/$value"
        return odata_path
    @prevent_server_error
    def _fetch_s2_qi_info(self) -> None:
        if self.api.is_online(self.uuid):
            path = f"{self.title}.SAFE/MTD_MSIL2A.xml"
            url = self._get_odata_file_url(path)
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
    def _download(self, band: list) -> dict:
        return self.api.download(self.uuid, directory_path=self.download_storage, checksum=False, nodefilter=make_path_filter(band))

    def download_image(self, image_database: Database, output_name: str = '', delete_temp_files: bool = False) -> None:
        self.path = image_database.full_path
        self.name = f'{output_name}_{self.format_date_as_str(current_date=self.date, return_format="%Y%m%d")}'
        
        images_folder = os.path.join(self.download_storage, self.filename)

        if not self.exists:
            filterB2 = "*_B02_10m*"
            filterB3 = "*_B03_10m*"
            filterB4 = "*_B04_10m*"
            filterB8 = "*_B08_10m*"
            bands_list = [filterB2, filterB3, filterB4, filterB8]
            
            downloaded_images = []
            for band in bands_list:
                downloaded_images.append(self._download(band=band))
            if not downloaded_images: return

            image_bands = self.get_files_by_extension(folder=images_folder, extension='.jp2')
            image_bands.reverse()
            CompositeBands(image_bands, self.full_path)

        if Exists(images_folder):
            Delete(images_folder)

class Image(BaseDBPath):
    _masked_prefix: str = 'Msk_'
    _mosaic_prefix: str = 'Mos_'
    _stretch_prefix: str = 'Stch_'
    _copy_prefix: str = 'Copy_'
    _classification_prefix: str = 'Clssif_'
    mosaic_dataset: MosaicDataset = None

    def __init__(self, path: str, name: str = None, images_for_composition: list = [], mask: Feature = None, compose_as_single_image: bool = True, stretch_image: bool = True, *args, **kwargs):
        self.date_processed: datetime = self.now
        self.date_created: datetime = self.now
        super(Image, self).__init__(path=path, name=name, *args, **kwargs)
        if images_for_composition:
            self.mosaic_images(images_for_composition=images_for_composition, compose_as_single_image=compose_as_single_image)
        if mask and isinstance(mask, Feature):
            self.extract_by_mask(area_of_interest=mask)
        if stretch_image:
            self.stretch_image()
    
    @delete_source_files
    @wrap_on_database_editing
    def mosaic_images(self, images_for_composition: list, compose_as_single_image: bool) -> str:
        list_of_images_paths = self.get_list_of_valid_paths(items=images_for_composition)

        if len(list_of_images_paths) == 1:
            image = list_of_images_paths[0]
            self.name = os.path.basename(image)
            self.path = os.path.dirname(image)
            return self.full_path

        self.name = f'{self._mosaic_prefix}{self.name}'
        
        if not compose_as_single_image:
            self.mosaic_dataset = MosaicDataset(path=self.full_path, images_for_composition=list_of_images_paths)
            self.path = os.path.dirname(self.mosaic_dataset.full_path)

        if self.exists:
            return self.full_path

        aprint(f'Criando Mosaico em {self.path}')
        MosaicToNewRaster(
            input_rasters=list_of_images_paths,
            output_location=self.path,
            raster_dataset_name_with_extension=self.name,
            number_of_bands=4,
            pixel_type='16_BIT_UNSIGNED',
            cellsize=10,
            mosaic_method='MAXIMUM',
            mosaic_colormap_mode='MATCH'
        )

        return self.full_path

    @delete_source_files
    @wrap_on_database_editing
    def extract_by_mask(self, area_of_interest: Feature) -> str:
        if self.mosaic_dataset: return self.full_path

        if Exists(os.path.join(self.path, f'{self._masked_prefix}{self.name}')):
            aprint(f'Encontrada imagem com máscara {self.full_path}')
            self.name = f'{self._masked_prefix}{self.name}'
            return self.full_path

        aprint(f'Extraindo máscara da imagem {self.full_path}')
        clipped_mosaic = ExtractByMask(
            in_raster=self.full_path,
            in_mask_data=area_of_interest.full_path
        )
        self.name = f'{self._masked_prefix}{self.name}'
        clipped_mosaic.save(self.full_path)

        return self.full_path
    
    @delete_source_files
    @wrap_on_database_editing
    def stretch_image(self) -> str:
        if Exists(os.path.join(self.path, f'{self._stretch_prefix}{self.name}')):
            aprint(f'Encontrada Imagem com Stretch - {self.full_path}')
            return self.full_path
        
        aprint(f'Aplicando Strech na Imagem {self.full_path}')
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
        self.name = f'{self._stretch_prefix}{self.name}'
        stretch.save(self.full_path)

        return self.full_path

    def copy_image(self, pixel_type: str = '', nodata_value: str = '', background_value: float = None, destination: str or Database = None, delete_source: bool = False, output_name: str = '') -> str:
        """Creates a copy of the current image
            Args:
                destination (str, optional): path to the folder to receive the raster. Defaults to 'IN_MEMORY'.
                pixel_type (str, optional): If none, the raster pixel type will be used. Defaults to None.
                > 1_BIT | 2_BIT | 4_BIT | 8_BIT_UNSIGNED | 8_BIT_SIGNED | 16_BIT_UNSIGNED | 16_BIT_SIGNED | 32_BIT_UNSIGNED | 32_BIT_SIGNED | 32_BIT_FLOAT | 64_BIT
            Returns:
                str: Path to the new copy
        """
        if not destination:
            destination = self.temp_db
        elif not isinstance(destination, Database):
            destination = Database(path=destination)

        if not output_name:
            output_name = f'{self._copy_prefix}{self.name}'

        if Exists(os.path.join(destination.full_path, output_name)):
            return os.path.join(destination.full_path, output_name)

        destination.start_editing()

        aprint(f'Criando cópia de {self.full_path}')
        CopyRaster(
            in_raster=self.full_path,
            out_rasterdataset=output_name,
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
        return os.path.join(destination.full_path, output_name)

    def get_image_nodata_area(self, output_path: str or Database = None):
        # Creates a black and white copy
        copy = self.copy_image(pixel_type='1_BIT', destination=output_path)
        return Feature(path=f'{copy}_polygon', raster=copy)

    def classify(self, classifier: BaseImageClassifier, output_path: Database, arguments: str = None, processor_type: str = 'CPU', n_cores: int = 1) -> Feature:
        aprint(f'Classificando a imagem {self.full_path}')
        classified_raster_full_path = os.path.join(self.temp_db.full_path, f'{self._classification_prefix}{self.name}')

        if not arguments:
            arguments="padding 70;batch_size 2;predict_background True;tile_size 256"

        if not Exists(classified_raster_full_path):
            self.temp_db.start_editing()
            with EnvManager(
                parallelProcessingFactor=str(n_cores),
                processorType=processor_type
            ):
                try:
                    out_classified_raster = ClassifyPixelsUsingDeepLearning(
                        in_raster=self.full_path,
                        in_model_definition=classifier.full_path,
                        arguments=arguments,
                        processing_mode="PROCESS_AS_MOSAICKED_IMAGE",
                        out_classified_folder=None
                    )
                    out_classified_raster.save(classified_raster_full_path)
                except Exception as e:
                    raise e
            self.temp_db.close_editing()
        self.date_processed = self.now
        polygon_path = f'{classified_raster_full_path}_polygon'
        if Exists(polygon_path):
            return Feature(polygon_path)

        feature = Feature(path=polygon_path, raster=classified_raster_full_path)
        feature.calculate_field(
            image_classifier=classifier,
            field_name=classifier.class_field
        )
        return feature

