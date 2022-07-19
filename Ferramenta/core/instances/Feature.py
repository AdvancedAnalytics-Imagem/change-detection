# -*- encoding: utf-8 -*-
#!/usr/bin/python
import json
import os
import time

from arcpy import (CopyFeatures_management, Describe, ExecuteError, Exists,
                   FeatureClassToFeatureClass_conversion,
                   FeaturesToJSON_conversion, GetCount_management, GetMessages,
                   ListFields, Project_management, RepairGeometry_management,
                   SelectLayerByAttribute_management,
                   SelectLayerByLocation_management, SpatialReference,
                   TruncateTable_management)
from arcpy.cartography import SimplifyLine, SimplifyPolygon
from arcpy.da import Editor, InsertCursor, SearchCursor, UpdateCursor
from arcpy.management import Delete
from core.libs.Base import (BaseConfig, BasePath, ProgressTracker,
                                   load_path_and_name)
from core.libs.ErrorManager import MaxFailuresError, UnexistingFeatureError
from nbformat import ValidationError
from arcpy.conversion import RasterToPolygon
from .Database import Database, wrap_on_database_editing
from .Editor import CursorManager

_REQUIRED_OVERLAP_TYPE_FOR_DISTANCE: list[str] = [
    'WITHIN_A_DISTANCE',
    'WITHIN_A_DISTANCE_GEODESIC',
    'WITHIN_A_DISTANCE_3D',
    'INTERSECT',
    'INTERSECT_3D',
    'HAVE_THEIR_CENTER_IN',
    'CONTAINS'
]

def retry_failed_attempts(wrapped_function):
    
    def wrapper(*args, **kwargs):
        _failed_ids = wrapped_function(*args, **kwargs)

        subsequent_failures = 0
        while _failed_ids:
            kwargs['data'] = _failed_ids
            kwargs['_remaining_records'] = True
            _failed_ids = wrapped_function(*args, **kwargs)
            subsequent_failures += 1

            if subsequent_failures > 20:
                raise MaxFailuresError(method=wrapped_function.__name__, attempts=subsequent_failures)

        return _failed_ids
    
    return wrapper

class BaseFeature(BasePath, CursorManager):
    database: Database = None
    temp_destination = 'IN_MEMORY'

    def __init__(self, path: str, name: str, *args, **kwargs):
        super(BaseFeature, self).__init__(path=path, name=name, *args, **kwargs)
        self.load_feature_variable(path=path, name=name)

    @load_path_and_name
    def load_feature_variable(self, path: str, name: str = None):
        """Loads a feature variable and guarantees it exists and, if in a GDB, if that GDB exists

            Args:
                path (str, optional): Path to the feature. Defaults to None.
                name (str): feature name

            Raises:
                UnexistingFeatureError

            Returns:
                self
        """
        if self.path != 'IN_MEMORY':
            if '.sde' in path or '.gdb' in path:
                self.database = Database(path=path)
                self.path = self.database.full_path
        
        self.full_path = os.path.join(self.path, self.name)


class Feature(BaseFeature):
    _fields: list = []
    _failed_ids: list = []
    _current_batch: list = []
    geometry_type: str = None
    OIDField: str = ''
    spatialReference: dict = None
    shape_field: str = ''
    raster_field: str = 'Value'

    @load_path_and_name
    def __init__(self, path: str, name: str = None, raster: str = None, temp_destination: str or Database = None, *args, **kwargs):
        if temp_destination:
            if not isinstance(temp_destination, Database):
                temp_destination = Database(temp_database)
            self.temp_destination = temp_destination

        if raster:
            self.create_polygon_from_raster(raster=raster, path=path, name=name)
        super(Feature, self).__init__(path=path, name=name, *args, **kwargs)
        if self.exists:
            description = Describe(self.full_path)
            self.geometry_type = description.shapeType if hasattr(description, 'ShapeType') else None
            self.OIDField = description.OIDFieldName if hasattr(description, 'OIDFieldName') else 'Id'
            self.shape_field = description.shapeFieldName if hasattr(description, 'shapeFieldName') else 'shape'
            self.spatialReference = description.spatialReference if hasattr(description, 'spatialReference') else {'name':'Unknown'}
            
    def __str__(self):
        return f'{self.name} - {self.geometry_type} > {self.full_path}'
    
    def row_count(self) -> int:
        return int(GetCount_management(in_rows=self.full_path)[0])

    @property
    def is_table(self):
        return not self.geometry_type

    @property
    def is_inside_database(self):
        return self.database is not None

    @property
    def field_names(self):
        if not self._fields:
            self._fields = self.get_field_names()
        return self._fields
    
    def repair_grometry(self):
        RepairGeometry_management(self.full_path)

    def get_geojson(self, out_sr=4326):
        feature = self.simpplify_geometry()
        path = os.path.join(self.database.base_path, self.name+'_temp_geojson.json')
        if Exists(path):
            Delete(path)
        response = FeaturesToJSON_conversion(feature, path, outputToWGS84=True)
        return response[0]

    @wrap_on_database_editing
    def simpplify_geometry(self, tolerance: int = 300):
        if self.geometry_type == 'Polygon':
            temp_feature = SimplifyPolygon(self.full_path, 'temp_simplify_'+self.name.replace('.shp',''), 'POINT_REMOVE', tolerance)
        elif self.geometry_type in ['Line', 'Polyline']:
            temp_feature = SimplifyLine(self.full_path, 'temp_simplify_'+self.name.replace('.shp',''), 'POINT_REMOVE', tolerance)
        return temp_feature[0]

    @wrap_on_database_editing
    def geojson_geometry(self, out_sr=4326):
        if self.is_table:
            return {}

        geojson = {
            "type": "FeatureCollection",
            "features": []
        }

        feature = self.simpplify_geometry()
        temp_feature = Project_management(feature, 'temp_project_'+self.name.replace('.shp',''), SpatialReference(out_sr))
        serialized_shapes = [item[0] for item in SearchCursor(temp_feature, ['SHAPE@'])]
        
        for shape in serialized_shapes:
            geometry = json.loads(shape.JSON)
            rings = geometry.get('rings')
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": self.geometry_type,
                    "coordinates": rings
                },
                "properties": {}
            }
            geojson['features'].append(feature)

        return geojson

    def select_by_attributes(self, where_clause: str) -> dict:
        return SelectLayerByAttribute_management(in_layer_or_view=self.full_path, where_clause=where_clause)

    def select_by_location(self, intersecting_feature: str, distance: int = None, overlap_type: str = 'INTERSECT') -> dict:
        """Returns and selects current feature by location, based on intersection with an intersecting feature
            Args:
                intersecting_feature (str): Feature that limits the selection of the current feature
                distance (int, optional): Selection distance. Defaults to None.
                overlap_type (str, optional): Type of overlap relation between the two features. Defaults to 'INTERSECT'.
            Returns:
                dict: Selected feature
        """
        if distance:
            if overlap_type not in _REQUIRED_OVERLAP_TYPE_FOR_DISTANCE:
                overlap_type = 'INTERSECT'
                distance = None

        intersecting_feature_path = ''
        if isinstance(intersecting_feature, str):
            intersecting_feature_path = intersecting_feature
        elif isinstance(intersecting_feature, self.__class__):
            intersecting_feature_path = intersecting_feature.full_path
        
        if not os.path.exists(intersecting_feature_path) and not Exists(intersecting_feature_path):
            raise UnexistingFeatureError(feature=intersecting_feature_path)
            
        selected_features = SelectLayerByLocation_management(
            in_layer=self.full_path,
            overlap_type=overlap_type,
            select_features=intersecting_feature_path,
            search_distance=distance
        )
        
        feature_name = self.get_unique_name(path=self.temp_destination, name=os.path.basename(self.name))
        return CopyFeatures_management(selected_features, os.path.join(self.temp_destination, feature_name))[0]

    
    def format_feature_field_structure(self, data: list, fields: list = [], format: str = tuple):
        """Formats data according to feature object
            Args:
                data (list): Feature
                format (str, optional): Type. Defaults to tuple.
            Returns:
                multiple: Format typer as per feature field structure and specified format
        """
        if format == dict:
            if not fields: fields = self.field_names
            return {field:data[index] for index, field in enumerate(fields)}
        if format == list:
            return list(data)
        return data

    def get_field_names(self, get_id: bool = False, get_shape: bool = True) -> list:
        """Returns all field names
            Args:
                get_id (bool, optional): Option to return OID field or not. Defaults to False.
                get_shape (bool, optional): Option to return Shape field or not. Defaults to True.
            Returns:
                list: Field names
        """
        field_names = [field.name for field in ListFields(self.full_path) if
            field.name != self.OIDField and
            self.shape_field not in field.name
        ]

        if self.shape_field and get_shape:
            field_names.append('SHAPE@')
        if self.OIDField and get_id:
            field_names.append(self.OIDField)
            
        return field_names

    def iterate_feature(self, fields: list[str] = ['*'], where_clause: str = None, sql_clause: tuple = (None,None), format: str = 'tuple'):
        """Iterates a feature and returns lines as they are read, according to field structure
            Args:
                fields (list[str], optional): Defaults to ['*'].
                where_clause (str, optional): Defaults to None.
                sql_clause (tuple, optional): Defaults to (None,None).
                format (str, optional): Cound be tuple, list or json. Defaults to 'tuple'.
            Yields:
                Iterator[tuple | list | dict]: row data on desired structure
        """
        if fields == ['*']:
            fields = self.get_field_names()
        with self.search_cursor(fields=fields, sql_clause=sql_clause, where_clause=where_clause) as selected_features:
            for selected_feature in selected_features:
                yield self.format_feature_field_structure(data=selected_feature, fields=fields, format=format)

    def serialize_feature_selection(self, fields: list = ['*'], where_clause: str = '1=1', sql_clause: tuple = (None,None), top_rows: int = None, oid_in: list = None) -> list[list]:
        """Return the rows as a list
            Args:
                fields (list, optional): Desired Fields. Defaults to ['*'].
                sql_clause (tuple, optional): Defaults to (None,None).
            Returns:
                list[list]: list or rows
        """
        def get_full_length_or_first(values=None):
            if values and isinstance(values, (tuple, list)) and len(values) == 1:
                return values[0]
            return values

        if oid_in:
            where_clause = f'{self.OIDField} in {tuple(oid_in)}'

        serialized_feature_rows = []
        for row in self.iterate_feature(fields=fields, where_clause=where_clause, sql_clause=sql_clause):
            serialized_feature_rows.append(get_full_length_or_first(values=row))
            if top_rows and len(serialized_feature_rows)==top_rows:
                return serialized_feature_rows
        return serialized_feature_rows

    def get_filtered_copy_feature(self, where_clause: str = None, top_rows: int = None, oid_in: list = None) -> dict:
        """Returns a IN_MEMORY copy of the feature fildered as per query
            Args:
                query (str, optional): Defaults to '1=1'.
            Raises:
                ValidationError: Error in case it's not possible to export feature to memory
            Returns:
                dict: Copied feature
        """
        if top_rows:
            top_ids = self.serialize_feature_selection(fields=[self.OIDField], top_rows=top_rows, oid_in=oid_in)
            where_clause = f'{self.OIDField} in {tuple(top_ids)}'
        try:
            feature_name = self.get_unique_name(path=self.temp_destination, name=os.path.basename(self.name))
            selection_copy = FeatureClassToFeatureClass_conversion(in_features=self.full_path, out_path=self.temp_destination, out_name=feature_name, where_clause=where_clause)
            return selection_copy[0]

        except Exception as e:
            raise ValidationError(message=f'Não foi possível exportar as feições selecionadas.\n{e}')

    def remove_all_rows(self):
        print(f'{self.row_count()} registros a serem removidos')
        while self.row_count():
            try:
                TruncateTable_management(in_table=self.full_path)
                continue
            except Exception as e:
                print(f'Unable to truncate table, trying via cursor.\nError:\n{e}')
            try:
                with self.update_cursor() as cursor:
                    for index, row in enumerate(cursor):
                        if self.batch_size and index and not index%self.batch_size:
                            break
                        cursor.deleteRow()
                self.database.close_editing()
            except Exception as e:
                time.sleep(self.regular_sleep_time_seconds)

        return True

    def append_dataset(self, origin, where_clause: str = None) -> list:
        current_feature_fields = self.get_field_names(get_id=False)
        
        total_records = origin.row_count()
        print(f'Anexando {total_records}')
        if not self.batch_size:
            self.batch_size = total_records

        self.progress_tracker.init_tracking(total=total_records, name='Append Data')
        count = 0

        for row_data in origin.iterate_feature(where_clause=where_clause, format=dict):
            self.insert_row(data=row_data, fields=current_feature_fields)
            count += 1
            self.progress_tracker.report_progress(current=count)
            
        self.insert_row(data=row_data, fields=current_feature_fields, _remaining_records=True)

    def map_data_to_field_structure(self, data: dict, field_names: any = None) -> list:
        return [data.get(field, None) for field in field_names]

    @retry_failed_attempts
    def insert_row(self, data: list, fields: list, _remaining_records: bool = False):
        if isinstance(data, list):
            self._current_batch.extend(data)
        else:
            self._current_batch.append(data)

        failed_ids = []
        if (self._current_batch and len(self._current_batch)%self.batch_size == 0) or _remaining_records:
            with self.insert_cursor(fields) as iCursor:
                for row_data in self._current_batch:
                    try:
                        reordered_data = self.map_data_to_field_structure(data=row_data, field_names=fields)
                        iCursor.insertRow(reordered_data)
                    except Exception as e:
                        failed_ids.append(row_data)
            del iCursor
            self._current_batch = []
            self.database.close_editing()

        return failed_ids

    def create_polygon_from_raster(self, raster: str or Image, path: str, name: str, raster_field: str = "Value"):
        if not isinstance(raster, str) and hasattr(raster, 'full_path'):
            raster = raster.full_path
        if not Exists(raster):
            raise UnexistingFeatureError(feature=raster)
        
        full_path = os.path.join(path, name)

        RasterToPolygon(
            in_raster=raster,
            out_polygon_features=full_path,
            simplify="SIMPLIFY",
            raster_field=raster_field,
            create_multipart_features="SINGLE_OUTER_PART",
            max_vertices_per_feature=None
        )
        self.raster_field = raster_field
        