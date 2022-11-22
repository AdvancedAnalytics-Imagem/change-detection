# -*- encoding: utf-8 -*-
#!/usr/bin/python
import os

from core._constants import *
from core._logs import *
from core.instances.Database import Database, load_path_and_name
from core.libs.BaseProperties import BaseProperties
from core.libs.CustomExceptions import PathINMEMORY


class BaseDBPath(BaseProperties):
    database: Database = None

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

            if self.database:
                self.path = self.database.full_path
        else:
            PathINMEMORY(feature = name)
