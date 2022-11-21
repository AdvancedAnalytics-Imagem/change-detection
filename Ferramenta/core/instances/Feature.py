# -*- encoding: utf-8 -*-
#!/usr/bin/python
import datetime
import json
import os
import time
from datetime import date, datetime

from arcpy import (Append_management, CopyFeatures_management, Describe,
                   Exists, FeatureClassToFeatureClass_conversion,
                   FeaturesToJSON_conversion, GetCount_management, ListFields,
                   MinimumBoundingGeometry_management, Project_management,
                   RepairGeometry_management,
                   SelectLayerByAttribute_management,
                   SelectLayerByLocation_management, SpatialReference,
                   TruncateTable_management)
from arcpy import env as arcpy_env
from arcpy.analysis import Intersect
from arcpy.cartography import SimplifyLine, SimplifyPolygon
from arcpy.conversion import RasterToPolygon
from arcpy.da import SearchCursor
from arcpy.management import AddField, CalculateField, Delete
from core._logs import *
from core.libs.Base import load_path_and_name
from core.libs.BaseDBPath import BaseDBPath
from core.libs.Enums import FieldType
from core.libs.CustomExceptions import (DatabaseInsertionError, MaxFailuresError,
                                    UnexistingFeatureError)
from core.ml_models.ImageClassifier import BaseImageClassifier
from nbformat import ValidationError

from .Database import Database, wrap_on_database_editing
from .Editor import CursorManager

_REQUIRED_OVERLAP_TYPE_FOR_DISTANCE = [
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

            if subsequent_failures > MaxFailuresError.max_failures:
                MaxFailuresError(method=wrapped_function.__name__, attempts=subsequent_failures)

        return _failed_ids
    
    return wrapper

class FieldManager:
    def get_field_type_by_value(self, field_value: any):
        if isinstance(field_value, datetime) or field_value == datetime or str(field_value) in ['Date']:
            return FieldType._date.value
        elif isinstance(field_value, date) or field_value == date:
            return FieldType._date.value
        elif isinstance(field_value, str) or field_value == str or str(field_value) in ['String']:
            return FieldType._str.value
        elif isinstance(field_value, int) or field_value == int or str(field_value) in ['SmallInteger', 'LongInteger', 'Integer']:
            return FieldType._short.value
        elif isinstance(field_value, float) or field_value == float or str(field_value) in ['Float']:
            return FieldType._float.value
        else:
            print(field_value)

    def get_structure(self, field_type: str = '', field_value: any = None) -> dict:
        """Gets field structure based on type or value
            Args:
                field_type (str, optional): Field type if known. Defaults to ''.
                field_value (any, optional): Value or data type. Defaults to None.
            Returns:
                dict: Field Structure to be used for field creation
                Needs to be added:
                field_precision,field_scale,field_length,field_alias,field_is_nullable,field_is_required,field_domain
        """
        if field_value:
            field_type = self.get_field_type_by_value(field_value=field_value)
        return {"field_type":field_type}


class Feature(BaseDBPath, CursorManager):
    _fields: list = []
    _failed_ids: list = []
    _current_batch: list = []
    geometry_type: str = None
    OIDField: str = ''
    spatialReference: dict = None
    shape_field: str = ''
    raster_field: str = 'Value'

    @load_path_and_name
    def __init__(self, path: str, name: str = None, raster: str = None, *args, **kwargs):
        if raster is not None:
            self.create_polygon_from_raster(raster=raster, path=path, name=name)

        super().__init__(path=path, name=name, *args, **kwargs)

        if self.exists:
            description = Describe(self.full_path)
            self.geometry_type = description.shapeType if hasattr(description, 'ShapeType') else None
            self.OIDField = description.OIDFieldName if hasattr(description, 'OIDFieldName') else 'Id'
            self.shape_field = description.shapeFieldName if hasattr(description, 'shapeFieldName') else 'shape'
            self.spatialReference = description.spatialReference if hasattr(description, 'spatialReference') else {'name':'Unknown'}
    
    def __repr__(self):
        return self.full_path

    def row_count(self) -> int:
        return int(GetCount_management(in_rows=self.full_path)[0])

    @property
    def is_table(self):
        return not self.geometry_type

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

    def create_geojson(self):
        feature = self.simpplify_geometry()
        if self.temp_db.full_path == 'IN_MEMORY':
            temp_folder = self.load_path_variable(ROOT_DIR, f'temp_{self.today_str}')
            temp_feature = Project_management(feature, os.path.join(temp_folder, f"temp_project_{self.name}"), SpatialReference(out_sr))
        else:
            temp_feature = Project_management(feature, f"temp_project_{self.name.replace('.shp','')}", SpatialReference(out_sr))
        
        for feature in SearchCursor(temp_feature, ['*']):
            print(feature)

    @wrap_on_database_editing
    def simpplify_geometry(self, tolerance: int = 300):
        if self.geometry_type == 'Polygon':
            temp_feature = SimplifyPolygon(self.full_path, 'temp_simplify_'+self.name.replace('.shp',''), 'POINT_REMOVE', tolerance)
        elif self.geometry_type in ['Line', 'Polyline']:
            temp_feature = SimplifyLine(self.full_path, 'temp_simplify_'+self.name.replace('.shp',''), 'POINT_REMOVE', tolerance)
        return temp_feature[0]

    @wrap_on_database_editing
    def geojson_geometry(self, out_sr=4326, temp_folder: str = None):
        if self.is_table:
            return {}
        geojson = {
            "type": "FeatureCollection",
            "features": []
        }

        feature = self.simpplify_geometry()
        if self.temp_db.full_path == 'IN_MEMORY':
            temp_folder = self.load_path_variable(ROOT_DIR, f'temp_{self.today_str}')
            temp_feature = Project_management(feature, os.path.join(temp_folder, f"temp_project_{self.name}"), SpatialReference(out_sr))
        else:
            temp_folder = ''
            temp_feature = Project_management(feature, f"temp_project_{self.name.replace('.shp','')}", SpatialReference(out_sr))

        for shape in SearchCursor(temp_feature, ['SHAPE@']):
            geometry = json.loads(shape[0].JSON)
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

        if temp_folder:
            Delete(temp_folder)
        Delete(temp_feature)

        return geojson

    def bounding_box(self, out_sr=4326) -> list:
        response = []

        if self.temp_db.full_path == 'IN_MEMORY':
            temp_folder = self.load_path_variable(ROOT_DIR, f'temp_{self.today_str}')
            temp_feature = Project_management(self.full_path, os.path.join(temp_folder, f"temp_project_{self.name}"), SpatialReference(out_sr))
        else:
            output_temp_feature = os.path.join(self.temp_db.full_path, f"temp_project_{self.name.replace('.shp','')}")
            if Exists(output_temp_feature):
                Delete(output_temp_feature)
            temp_folder = ''
            temp_feature = Project_management(
                self.full_path,
                output_temp_feature,
                SpatialReference(out_sr)
            )

        for feature in SearchCursor(temp_feature, ['SHAPE@']):
            response = [feature[0].extent.XMin, feature[0].extent.YMin, feature[0].extent.XMax, feature[0].extent.YMax]

        if temp_folder:
            Delete(temp_folder)
        Delete(temp_feature)

        return response

    def select_by_attributes(self, where_clause: str) -> dict:
        return SelectLayerByAttribute_management(in_layer_or_view=self.full_path, where_clause=where_clause)

    def select_by_location(
        self,
        intersecting_feature: str,
        distance: int = None,
        overlap_type: str = 'INTERSECT',
        in_memory: bool = False
    ) -> dict:
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
        
        feature_name = self.get_unique_name(path=self.temp_db, name=os.path.basename(self.name))
        
        if in_memory:
            output_selected_feature = os.path.join('IN_MEMORY', feature_name)
        else:
            output_selected_feature = os.path.join(self.temp_db.full_path, feature_name)

        return CopyFeatures_management(selected_features, output_selected_feature)[0]

    def format_feature_field_structure(self, data: list, fields: list = [], format: str = tuple, field_map: dict = {}):
        """Formats data according to feature object
            Args:
                data (list): Feature
                format (str, optional): Type. Defaults to tuple.
            Returns:
                multiple: Format typer as per feature field structure and specified format
        """
        if format == dict:
            if not fields: fields = self.field_names
            return {field_map.get(field, field):data[index] for index, field in enumerate(fields)}

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
        if not self.exists:
            raise UnexistingFeatureError(feature=self.full_path)
        field_names = [field.name for field in ListFields(self.full_path) if
            field.name != self.OIDField and
            self.shape_field not in field.name
        ]

        if self.shape_field and get_shape:
            field_names.append('SHAPE@')
        if self.OIDField and get_id:
            field_names.append(self.OIDField)
            
        return field_names

    def get_field_structure(self, lowercase: bool = False) -> dict:
        if not self.exists:
            raise UnexistingFeatureError(feature=self.full_path)
        if lowercase:
            return {f.name.lower():f.type for f in ListFields(self.full_path)}
        return {f.name:f.type for f in ListFields(self.full_path)}

    def iterate_feature(self, fields: list = ['*'], where_clause: str = None, sql_clause: tuple = (None,None), format: str = 'tuple', lower_case_fields: bool = False, field_map: dict = {}):
        """Iterates a feature and returns lines as they are read, according to field structure
            Args:
                fields (list, optional): Defaults to ['*'].
                where_clause (str, optional): Defaults to None.
                sql_clause (tuple, optional): Defaults to (None,None).
                format (str, optional): Cound be tuple, list or json. Defaults to 'tuple'.
            Yields:
                Iterator[tuple | list | dict]: row data on desired structure
        """
        if not self.exists:
            raise UnexistingFeatureError(feature=self.full_path)
        if fields == ['*']:
            fields = self.get_field_names(get_id=True)
            if lower_case_fields:
                fields = list(map(lambda x: x.lower(), fields))
        with self.search_cursor(fields=fields, sql_clause=sql_clause, where_clause=where_clause) as selected_features:
            for selected_feature in selected_features:
                yield self.format_feature_field_structure(data=selected_feature, fields=fields, format=format, field_map=field_map)
        del selected_features, selected_feature

    def serialize_feature_selection(self, fields: list = ['*'], where_clause: str = '1=1', sql_clause: tuple = (None,None), top_rows: int = None, oid_in: list = None) -> list:
        """Return the rows as a list
            Args:
                fields (list, optional): Desired Fields. Defaults to ['*'].
                sql_clause (tuple, optional): Defaults to (None,None).
            Returns:
                list: list or rows
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
            feature_name = self.get_unique_name(path=self.temp_db, name=os.path.basename(self.name))
            selection_copy = FeatureClassToFeatureClass_conversion(in_features=self.full_path, out_path=self.temp_db, out_name=feature_name, where_clause=where_clause)
            return selection_copy[0]

        except Exception as e:
            raise ValidationError(message=f'Não foi possível exportar as feições selecionadas.\n{e}')

    def remove_all_rows(self):
        aprint(f'{self.row_count()} registros a serem removidos')
        while self.row_count():
            try:
                TruncateTable_management(in_table=self.full_path)
                continue
            except Exception as e:
                aprint(f'Unable to truncate table, trying via cursor.\nError:\n{e}')
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

    def look_for_missing_fields(self, fields: dict) -> dict:
        """Returns the dict of the fields that exist or could be created on the current feature
            Args:
                fields (dict): Fields and field values to be added
                {field_name:field_value}
            Returns:
                dict: Fields that exist on current feature
        """
        def stripped_name(name):
            return name.strip().lower().replace('_','').replace('-','')
        
        all_fields = {stripped_name(field):field
                                    for field in self.get_field_names()}

        field_names = list(fields.keys())

        for field_name in field_names:
            if stripped_name(field_name) in all_fields:
                continue
            success = self.add_field(field_name=field_name, field_value=fields.get(field_name))
            if not success:
                fields.pop(field_name)
        return fields
            
    def add_field(self, field_name: str, field_type: str = '', field_value: any = None) -> bool:
        if not field_type and field_value is None:
            return False
        
        field_structure = FieldManager().get_structure(field_value=field_value)
        try:
            AddField(
                in_table=self.full_path,
                field_name=field_name,
                **field_structure
            )
            return True
        except Exception as e:
            aprint(f'Erro ao adicionar o campo {field_name},\n{e}')
            return False

    def _append(self, origin):
        origin = [o.full_path for o in origin]
        Append_management(
            inputs=origin,
            target=self.full_path,
            schema_type='NO_TEST'
        )

    def regular_append(self, origin, where_clause: str = None, extra_constant_values: dict = {}, field_map: dict = {}):
        def stripped_name(name):
            return name.strip().lower().replace('_','').replace('-','')

        if not isinstance(origin, list):
            origin = [origin]
        
        dest_feature_field_names = {stripped_name(field):field
                       for field in self.get_field_names()}

        for feature in origin:
            if extra_constant_values:
                for field in extra_constant_values:
                    field_name = dest_feature_field_names.get(stripped_name(field), field)
                    feature.calculate_field(
                        field_name=field_name,
                        field_value=extra_constant_values[field]
                    )

            if field_map:
                field_structure = feature.get_field_structure(lowercase=True)
                for field in field_map:
                    new_field_name = dest_feature_field_names.get(stripped_name(field), field)
                    original_field = field_map.get(field)
                    original_field_value = field_structure.get(original_field)
                    
                    expression=f"!{original_field}!"
                    code_block=""

                    feature.calculate_field(
                        field_name=new_field_name,
                        field_value=original_field_value,
                        expression=expression,
                        code_block=code_block)

        self._append(origin=origin)

    def append_dataset(self, *args, **kwargs) -> list:
        if self.use_arcpy_append:
            self.regular_append(*args, **kwargs)
        else:
            self.cursor_append(*args, **kwargs)
    
    def cursor_append(self, origin, where_clause: str = None, extra_constant_values: dict = {}, field_map: dict = {}):
        if extra_constant_values:
            extra_constant_values = self.look_for_missing_fields(extra_constant_values)

        fields = list(self.get_field_names())
        fields = list(map(lambda x: x.lower(), fields))
        
        total_records = origin.row_count()
        
        self._current_batch = []
        aprint(f'Anexando {total_records} de {origin.name} em {self.name}')
        if not self.batch_size: self.batch_size = total_records

        self.progress_tracker.init_tracking(total=total_records, name='Append Data')
        for row_data in origin.iterate_feature(where_clause=where_clause, format=dict, lower_case_fields=True, field_map=field_map):
            reordered_data = self.map_data_to_field_structure(data={**row_data, **extra_constant_values}, field_names=fields)
            self.insert_row(data=reordered_data, fields=fields)
        
        self.insert_row(data=reordered_data, fields=fields, _remaining_records=True)

    @retry_failed_attempts
    def insert_row(self, data: list, fields: list, _remaining_records: bool = False):
        if isinstance(data, list) and isinstance(data[0], list):
            self._current_batch.extend(data)
        elif not _remaining_records:
            self._current_batch.append(data)
        failed_ids = []

        if (self._current_batch and len(self._current_batch)%self.batch_size == 0) or _remaining_records:
            aprint(f'>> Linhas a serem inseridas: {len(self._current_batch)}')
            with self.insert_cursor(fields) as iCursor:
                for row_data in self._current_batch:
                    if not isinstance(row_data, list):
                        continue

                    try:
                        iCursor.insertRow(row_data)
                        self.progress_tracker.report_progress(add_progress=True)
                    except Exception as e:
                        failed_ids.append(row_data)

            del iCursor, row_data
            self._current_batch = []
            self.database.close_editing()

        return failed_ids

    def create_polygon_from_raster(self, raster: str, path: str, name: str, raster_field: str = "Value"):
        if not isinstance(raster, str) and hasattr(raster, 'full_path'):
            raster = raster.full_path
        if not Exists(raster):
            raise UnexistingFeatureError(feature=raster)
        
        full_path = os.path.join(path, name)
        if not Exists(full_path):
            RasterToPolygon(
                in_raster=raster,
                out_polygon_features=full_path,
                simplify="SIMPLIFY",
                raster_field=raster_field,
                create_multipart_features="SINGLE_OUTER_PART",
                max_vertices_per_feature=None
            )
        self.raster_field = raster_field
    
    @wrap_on_database_editing
    def update_rows(self, where_clause: str, fields: list, values: list) -> None:
        self.look_for_missing_fields(fields={fields[i]:values[i] for i in range(len(fields))})
        with self.update_cursor(
            fields=fields,
            where_clause=where_clause) as cursor:
            for row in cursor:
                cursor.updateRow(values)

    def calculate_field(self, field_name: str, field_value: any = str, expression: str = None, code_block: str = None, expression_type: str = "PYTHON3", image_classifier: BaseImageClassifier = None):
        aprint(f'Calculando campo {field_name}')
        if image_classifier is not None:
            self.look_for_missing_fields(fields={image_classifier.class_field:str})
            for classified_class in image_classifier.Classes:
                with self.update_cursor(
                    fields=image_classifier.class_field,
                    where_clause=f'gridcode = {classified_class.value.value}') as cursor:
                    for row in cursor:
                        row[0] = classified_class.value.label
                        cursor.updateRow(row)
        elif expression:
            CalculateField(
                in_table=self.full_path,
                field=field_name,
                expression=expression,
                expression_type=expression_type,
                code_block=code_block,
                field_type=FieldManager().get_field_type_by_value(field_value=field_value)
            )
        else:
            field_type = FieldManager().get_field_type_by_value(field_value=field_value)
            expression=f"func('{field_value}', '{field_type}')"
            code_block="""def func(value, type):
                    if type == 'DATE':
                        try:
                            return datetime.datetime.strptime(value, '%Y-%m-%d')
                        except:
                            return datetime.strptime(value, '%Y-%m-%d')
                    if type == 'SHORT':
                        return int(value)
                    return value"""
            CalculateField(
                in_table=self.full_path,
                field=field_name,
                expression=expression,
                expression_type=expression_type,
                code_block=code_block,
                field_type=FieldManager().get_field_type_by_value(field_value=field_value)
            )


    def intersects(self, intersecting_feature: str):
        if not isinstance(intersecting_feature, str) and hasattr(intersecting_feature, 'full_path'):
            intersecting_feature = intersecting_feature.full_path

        intersect_priority = [
            [intersecting_feature, 0],
            [self.full_path, 1]
        ]
        output = os.path.join(self.temp_db.full_path, f'intersection_{self.today_str}')
        if Exists(output):
            return output

        return Intersect(
            in_features=intersect_priority,
            out_feature_class=output,
            join_attributes="ALL",
            cluster_tolerance=None,
            output_type="INPUT"
        )[0]

    @staticmethod
    def map_data_to_field_structure(data: dict, field_names: any = None) -> list:
        return [data.get(field.lower(), None) for field in field_names]
