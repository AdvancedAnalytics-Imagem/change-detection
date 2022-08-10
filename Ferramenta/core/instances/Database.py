# -*- encoding: utf-8 -*-
#!/usr/bin/python
import os
import time

from arcpy import (CheckOutExtension, CreateFeatureDataset_management,
                   CreateFileGDB_management, Exists)
from arcpy import env as arcpy_env
from arcpy.da import Editor
from core._constants import *
from core._logs import *
from core.libs.Base import (BaseConfig, BasePath, ProgressTracker,
                            load_path_and_name)
from core.libs.ErrorManager import UnexistingGDBConnectionError


class SessionManager(BasePath, BaseConfig):
    """Editing session of a database, supose to be activated every time a feature/table will be edited inside a database
        Raises:
            NotADatabase: Error for when an editing session is started and the target is not a database
    """
    session: Editor = None
    is_editing: bool = False
    _previous_workspace: str = None
    full_path: str = None
    
    def __init__(self, *args, **kwargs):
        super(SessionManager, self).__init__(*args, **kwargs)

    @property
    def is_gdb(self) -> bool:
        return self.full_path.endswith('.gdb')

    @property
    def workspace(self):
        return arcpy_env.workspace
        
    @property
    def is_sde(self) -> bool:
        return self.full_path.endswith('.sde')

    def set_env_configs(self) -> None:
        arcpy_env.addOutputsToMap = False
        arcpy_env.overwriteOutput = True
        CheckOutExtension("ImageAnalyst")

    def set_workspace_env(self):
        self._previous_workspace = arcpy_env.workspace
        arcpy_env.workspace = self.full_path
        self.set_env_configs()

    def revert_workspace_env(self):
        arcpy_env.workspace = self._previous_workspace
        self.set_env_configs()

    @property
    def session(self):
        if not self.is_sde:
            return None

        if not self._current_session:
            self._current_session = Editor(full_path=self.full_path)
        
        return self._current_session

    def start_editing(self) -> None:
        self.set_workspace_env()

        if self.is_editing:
            self.close_editing
            
        self.is_editing = True

        if not self.is_sde:
            return

        self.session.startEditing(
            with_undo = True,
            multiuser_mode = True
        )
        self.session.startOperation()

    def close_editing(self) -> None:
        if self.is_editing and self.session:
            try:
                self.session.stopOperation()
                self.session.stopEditing(save_changes = True)
            except Exception as e:
                raise SavingEditingSessionError(session=self.session)
        self.is_editing = False
        self.revert_workspace_env()
    
    def refresh_session(self):
        self.close_editing()
        self.start_editing()


class Database(SessionManager):
    feature_dataset : str = ''
    
    def __init__(self, path : str, name : str = None, create: bool = True, *args, **kwargs) -> None:
        super(Database, self).__init__(path=path, name=name, *args, **kwargs)
        self.load_gdb_sde_variable(path=path, name=name, create=create)

    def __str__(self):
        return f'DB {self.name} > {self.full_path}'
        
    @load_path_and_name
    def load_gdb_sde_variable(self, path : str, name : str = None, create : bool = True) -> None:
        """Loads GDB or SDE connection string and guarantees it exists, and ends with the propper strin sulfix.
            In case the sulfix isn't one of the two options, it creates a '.gdb'
            Args:
                path (str): Path to GDB/SDE connection or direct path to it
                name (str, optional): GDB/SDE name. Defaults to None
                create (bool, optional): Option to create a GDB if it doesn't exist. Defaults to True
            Raises:
                UnexistingSDEConnectionError
                UnexistingGDBConnectionError
        """
        if path == 'IN_MEMORY':
            self.name = ''
            self.path = path
            self.full_path = path
            return

        if '.sde' not in self.full_path and '.gdb' not in self.full_path:
            self.name += '.gdb' # Not a GDB or SDE, assume its a GDB
        elif not self.full_path.endswith('.sde') and not self.full_path.endswith('.gdb'):
            # In this case, it is either a GDB or an SDE but it doesn't end with the sulfix, so it's a feature dataset
            self.load_featureDataset_variable(path, self.name)
            self.name = os.path.basename(path)
            path = os.path.dirname(path)
            
        self.load_base_path_variables(path)

        if not Exists(self.full_path):
            if self.name.endswith('.sde'):
                raise UnexistingSDEConnectionError(sde=self.full_path)
            try:
                if create:
                    CreateFileGDB_management(path, self.name)
                else: raise
            except Exception as e:
                raise UnexistingGDBConnectionError(gdb=self.full_path)
        
        if self.feature_dataset:
            self.full_path = os.path.join(self.full_path, self.feature_dataset)

    @load_path_and_name
    def load_featureDataset_variable(self, path : str, name : str, sr : int = 4326) -> None:
        """Load Feature Dataset variables from setting to guarantee they exist and are acessible
            Args:
                path (str): GDB/SDE path ending in '.sde' or '.gdb'
                name (str): Feature Dataset Name
                sr (int, optional): Spatial Reference. Defaults to 4326.
            Raises:
                CreateFeatureDataset_management,
                UnexistingFeatureDatasetError
        """
        feature_dataset_full_path = os.path.join(path, name)
        
        if not Exists(path):
            try:
                CreateFeatureDataset_management(out_dataset_path=path, out_name=name, sr=sr)
            except Exception as e:
                raise UnexistingFeatureDatasetError(feature_dataset=feature_dataset_full_path)

        self.feature_dataset = name


def wrap_on_database_editing(wrapped_function):
    def editor_wrapper(self, *args, **kwargs):
        if self.is_inside_database:
            self.database.start_editing()
        else:
            self.temp_destination.start_editing()

        result = wrapped_function(self, *args, **kwargs)
        
        if self.is_inside_database:
            self.database.close_editing()
        else:
            self.temp_destination.start_editing()

        return result
    
    return editor_wrapper

class BaseDatabasePath(BasePath):
    database: Database = None
    temp_destination = 'IN_MEMORY'

    def __init__(self, path: str, name: str, create: bool = False, *args, **kwargs):
        super().__init__(path=path, name=name, *args, **kwargs)
        self.load_database_path_variable(path=path, name=name, create=create)

    @property
    def is_inside_database(self):
        return self.database is not None
        
    @load_path_and_name
    def load_database_path_variable(self, path: str, name: str = None, create: bool = False):
        """Loads a feature variable and guarantees it exists and, if in a GDB, if that GDB exists
            Args:
                path (str, optional): Path to the feature. Defaults to None.
                name (str): feature name
        """
        if self.path != 'IN_MEMORY':
            if create:
                if path.endswith('.sde') or path.endswith('.gdb'):
                    self.database = Database(path=path, create=create)
                else:
                    self.database = Database(path=path, name=name, create=create)

            if '.sde' in path or '.gdb' in path:
                self.database = Database(path=path)

            self.path = self.database.full_path
        
        self.full_path = os.path.join(self.path, self.name)
