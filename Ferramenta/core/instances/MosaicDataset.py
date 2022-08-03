# -*- coding: utf-8 -*-
#!/usr/bin/python
import os

from arcpy import (CreateMosaicDataset_management, Describe, Exists,
                   SpatialReference)
from arcpy.management import AddRastersToMosaicDataset
from core.libs.Base import BaseConfig, load_path_and_name, prevent_server_error
from core.libs.ErrorManager import MosaicDatasetError

from .Database import BaseDatabasePath, Database, wrap_on_database_editing


class MosaicDataset(BaseDatabasePath, BaseConfig):
    prefix: str = 'MosDtst_'
    _coordinate_system: SpatialReference = None

    @load_path_and_name
    def __init__(self, path: str, name: str = None, images_for_composition: list = [], *args, **kwargs):
        name = f'{self.prefix}{name}'
        super().__init__(path=path, name=name, *args, **kwargs)

        if images_for_composition:
            self._coordinate_system = Describe(images_for_composition[0]).spatialReference
        
        if not self.exists:
            self.create_mosaic_dataset()
        
        if images_for_composition:
            self.add_images(images=images_for_composition)
    
    @property
    def coordinate_system(self):
        if not self._coordinate_system:
            self._coordinate_system = Describe(self.full_path).spatialReference
        return self._coordinate_system

    @wrap_on_database_editing
    def create_mosaic_dataset(self):
        try:
            CreateMosaicDataset_management(
                in_workspace=self.database.full_path,
                in_mosaicdataset_name=self.name,
                coordinate_system=self.coordinate_system,
            )
        except Exception as e:
            raise MosaicDatasetError(mosaic=self.full_path, error=e, message='Erro ao criar Mosaic Dataset')

    @wrap_on_database_editing
    def add_images(self, images):
        images = self.get_list_of_valid_paths(items=images)
        try:
            AddRastersToMosaicDataset(
                in_mosaic_dataset=self.full_path,
                raster_type="Raster Dataset",
                input_path=images,
                duplicate_items_action='EXCLUDE_DUPLICATES',
                build_pyramids=True,
                calculate_statistics=True,
                force_spatial_reference=True,
                estimate_statistics=True
            )
        except Exception as e:
            raise MosaicDatasetError(mosaic=self.full_path, error=e, message='Erro ao adicionar imagens ao Mosaic Dataset')
