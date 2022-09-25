# -*- coding: utf-8 -*-
#!/usr/bin/python
import os

from arcpy import (CreateMosaicDataset_management, Describe, Exists, ListDatasets,
                   SpatialReference)
from arcpy.management import AddRastersToMosaicDataset
from core._logs import *
from core.libs.Base import load_path_and_name, prevent_server_error
from core.libs.BaseDBPath import BaseDBPath
from core.libs.ErrorManager import MosaicDatasetError
from core.instances.Feature import Feature
from .Database import Database, wrap_on_database_editing


class MosaicDataset(BaseDBPath):
    prefix: str = 'MosDtst_'
    _coordinate_system: SpatialReference = SpatialReference(4326) # GCS_WGS_1984

    @load_path_and_name
    def __init__(self, path: str, name: str = None, images_for_composition: list = [], *args, **kwargs) -> None:
        super().__init__(path=path, name=name, *args, **kwargs)

        if images_for_composition:
            self._coordinate_system = Describe(images_for_composition[0]).spatialReference
        
        if not self.exists:
            if not name.startswith(self.prefix):
                name = f'{self.prefix}{name}'
            self.create_mosaic_dataset()
        
        if images_for_composition:
            self.add_images(images=images_for_composition)
        
        try:
            footprints_name = list(
                filter(
                    lambda n : n.endswith('_CAT'),
                    Describe(self.full_path).childrenNames
                )
            )
            if footprints_name:
                self.footprints_name = footprints_name[0]
        except Exception as e:
            self.footprints_name = f'AMD_{self.name}_CAT'
        self.footprints_layer = Feature(path=self.path, name=self.footprints_name)
        
    @property
    def coordinate_system(self) -> SpatialReference:
        if not self._coordinate_system:
            self._coordinate_system = Describe(self.full_path).spatialReference
        return self._coordinate_system

    @wrap_on_database_editing
    def create_mosaic_dataset(self) -> None:
        try:
            CreateMosaicDataset_management(
                in_workspace=self.database.full_path,
                in_mosaicdataset_name=self.name,
                coordinate_system=self.coordinate_system,
            )
        except Exception as e:
            raise MosaicDatasetError(mosaic=self.full_path, error=e, message='Erro ao criar Mosaic Dataset')

    @wrap_on_database_editing
    def add_images(self, images: str) -> None:
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
