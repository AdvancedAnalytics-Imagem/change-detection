# -*- encoding: utf-8 -*-
#!/usr/bin/python

from arcpy import Extent, ListTransformations, SpatialReference
from arcpy.da import InsertCursor, SearchCursor, UpdateCursor
from core._constants import *
from core.libs.CustomExceptions import NotInADatabase, SavingEditingSessionError

from .Database import Database

# TODO Create transformation manager for insert cursor
# class TransformationManager:

#     from_sr = arcpy.SpatialReference('WGS 1984')
#     to_sr = arcpy.SpatialReference('NAD 1927 StatePlane California VI FIPS 0406')

#     extent = arcpy.Extent(-178.217598182, 18.9217863640001,
#                         -66.969270909, 71.4062354550001)
#     transformations = arcpy.ListTransformations(from_sr, to_sr, extent)

class CurrentCursor:
    kwargs: dict = None
    method: callable = None

    def __init__(self, method: callable, *args, **kwargs):
        self.method = method
        self.kwargs = kwargs
        
    def open_cursor(self):
        return self.method(**self.kwargs)

class CursorManager:
    current_cursor = None
    database: Database = None

    def insert_cursor(self, fields: list = ['*'], datum_transformation: ListTransformations = None) -> dict:
        self.database.start_editing()
        insert_cursor_args = {
            'in_table':self.full_path,
            'field_names':fields,
            'datum_transformation':datum_transformation
        }
        self.current_cursor = CurrentCursor(method=InsertCursor, **insert_cursor_args)
        return self.current_cursor.open_cursor()

    def update_cursor(self, fields: list = ['*'], where_clause: str = None, spatial_reference: SpatialReference = None, explode_to_points: bool = False, sql_clause: tuple = (None, None), datum_transformation: ListTransformations = None) -> dict:
        self.database.start_editing()
        update_cursor_args = {
            'in_table':self.full_path,
            'field_names':fields,
            'where_clause':where_clause,
            'spatial_reference':spatial_reference,
            'explode_to_points':explode_to_points,
            'sql_clause':sql_clause,
            'datum_transformation':datum_transformation
        }
        self.current_cursor = CurrentCursor(method=UpdateCursor, **update_cursor_args)
        return self.current_cursor.open_cursor()

    def search_cursor(self, fields: list = ['*'], where_clause: str = None, spatial_reference: SpatialReference = None, explode_to_points: bool = False, sql_clause: tuple = (None, None), datum_transformation: ListTransformations = None) -> dict:
        search_cursor_args = {
            'in_table':self.full_path,
            'field_names':fields,
            'where_clause':where_clause,
            'spatial_reference':spatial_reference,
            'explode_to_points':explode_to_points,
            'sql_clause':sql_clause,
            'datum_transformation':datum_transformation
        }
        self.current_cursor = CurrentCursor(method=SearchCursor, **search_cursor_args)
        return self.current_cursor.open_cursor()
