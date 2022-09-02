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
from core.libs.Base import BasePath, ProgressTracker, load_path_and_name
from core.libs.ErrorManager import (SavingEditingSessionError,
                                    UnexistingFeatureDatasetError,
                                    UnexistingGDBConnectionError,
                                    UnexistingSDEConnectionError)


class SessionManager(BasePath):
    """Editing session of a database, supose to be activated every time a feature/table will be edited inside a database"""
    session: Editor = None
    is_editing: bool = False
    _previous_workspace: str = None
    _current_session: Editor = None

    @property
    def is_gdb(self) -> bool:
        return '.gdb' in self.full_path

    @property
    def workspace(self):
        return arcpy_env.workspace
        
    @property
    def is_sde(self) -> bool:
        return '.sde' in self.full_path

    def set_env_configs(self) -> None:
        arcpy_env.addOutputsToMap = False
        arcpy_env.overwriteOutput = True
        CheckOutExtension("ImageAnalyst")
        CheckOutExtension("DataInteroperability")
        CheckOutExtension("Spatial")

    def set_workspace_env(self):
        if arcpy_env.workspace:
            self._previous_workspace = str(arcpy_env.workspace)
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
            self._current_session = Editor(self.full_path)
        
        return self._current_session

    def start_editing(self, previous_workspace: str = None) -> None:
        self.set_workspace_env()

        if self.is_editing:
            self.close_editing()
            
        self.is_editing = True

        if not self.is_sde:
            return

        self.session.startEditing(
            with_undo = False, # True in case database is versioned 
            multiuser_mode = False # True in case database is versioned
        )
        self.session.startOperation()

    def close_editing(self) -> None:
        if self.is_editing and self.session:
            try:
                self.session.stopOperation()
                self.session.stopEditing(save_changes = True)
            except Exception as e:
                raise SavingEditingSessionError(session=self)
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
                UnexistingSDEConnectionError; UnexistingGDBConnectionError
        """
        if path == 'IN_MEMORY':
            self.name = ''
            self.path = path
            return

        if '.sde' not in self.full_path and '.gdb' not in self.full_path:
            self.name += '.gdb' # Not a GDB or SDE, assume its a GDB
        elif not self.full_path.endswith('.sde') and not self.full_path.endswith('.gdb'):
            # In this case, it is either a GDB or an SDE but it doesn't end with the sulfix, so it's a feature dataset
            self.load_featureDataset_variable(path, self.name)
            self.name = os.path.basename(path)
            path = os.path.dirname(path)
            
        self.load_path_variable(path)

        if not Exists(self.full_path):
            if self.name.endswith('.sde'):
                raise UnexistingSDEConnectionError(sde=self.full_path)
            try:
                if create:
                    CreateFileGDB_management(path, self.name)
                else: raise
            except Exception as e:
                raise UnexistingGDBConnectionError(gdb=self.full_path, error=e)
        
        if self.feature_dataset:
            self.feature_dataset_full_path = os.path.join(self.full_path, self.feature_dataset)

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
                raise UnexistingFeatureDatasetError(feature_dataset=feature_dataset_full_path, error=e)

        self.feature_dataset = name


def wrap_on_database_editing(wrapped_function):
    def editor_wrapper(self, *args, **kwargs):
        if self.is_inside_database:
            self.database.start_editing()
        else:
            self.temp_db.start_editing()

        result = wrapped_function(self, *args, **kwargs)
        
        if self.is_inside_database:
            self.database.close_editing()
        else:
            self.temp_db.close_editing()

        return result
    
    return editor_wrapper
