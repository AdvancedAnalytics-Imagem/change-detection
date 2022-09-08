# -*- encoding: utf-8 -*-
#!/usr/bin/python
import sys

from core._logs import *


class Error(Exception):
    interrupt_execution: bool = True

    """Base class for other exceptions"""
    def __init__(self, message: str):
        if self.interrupt_execution:
            aprint(message, level=LogLevels.CRITICAL)
            aprint('Interrompendo execução', level=LogLevels.WARNING)
            sys.exit()
        else:
            aprint(message, level=LogLevels.INFO)

class FolderAccessError(Error):
    """Exception for an unaccessible folder"""

    def __init__(self, folder, error: Error = ''):
        self.message = f'Não foi possível acessar o diretório {folder}.\n{error}'
        super().__init__(self.message)

class UnexistingFeatureError(Error):
    """Exception for layers and features that don't exist"""

    def __init__(self, feature, error: Error = '') -> None:
        self.feature = feature
        self.message = f'Não foi possível acessar a feição {feature}.\n{error}'
        super().__init__(self.message)

class UnexistingSDEConnectionError(Error):
    """Exception for SDE connection that doesn't exist"""

    def __init__(self, sde: str, error: Error = '') -> None:
        self.sde = sde
        self.message = f'Não foi possível acessar a conexão SDE {sde}.\n{error}'
        super().__init__(self.message)

class UnexistingGDBConnectionError(Error):
    """Exception for GDB that doesn't exist"""

    def __init__(self, gdb: str, error: Error = '') -> None:
        self.gdb =  gdb
        self.message = f'Não foi possível acessar/criar o GDB {gdb}.\n{error}'
        super().__init__(self.message)

class UnexistingFeatureDatasetError(Error):
    """Exception for GDB that doesn't exist"""

    def __init__(self, feature_dataset : str, error: Error = '') -> None:
        self.feature_dataset =  feature_dataset
        self.message = f'Não foi possível acessar/criar o Feature Dataset {feature_dataset}.\n{error}'
        super().__init__(self.message)

class NotInADatabase(Error):
    """Exception for when a feature is not inside a Database"""

    def __init__(self, path : str, error: Error = '') -> None:
        self.database = path
        self.message = f'{path} não é em um GDB ou SDE.\n{error}'
        super().__init__(self.message)

class SavingEditingSessionError(Error):
    """Handler for saving editing on a database when session is closed"""

    def __init__(self, session: str, error: Error = ''):
        self.sesison = session
        self.message = f'Não foi possível salvar as edições no banco {session}.\n{error}'
        super().__init__(self.message)

class MaxFailuresError(Error):
    """Handler for failed attempts that exceed a maximum limit"""
    max_failures: int = 60 # Maximum number or attempts
    wait_time_seconds: int = 10 # Time increased per attempt (total time until timeout == 60*10 = 5h 5min)
    
    def __init__(self, method: str, attempts: int, error: Error = ''):
        self.method = method
        self.attempts = attempts
        self.message = f'Timeout Error.\nO método {method} falhou {attempts-1} vezes consecutivas.\n{error}'
        super().__init__(self.message)

class MosaicDatasetError(Error):
    """Handles errors on Mosaic Datasets"""

    def __init__(self, mosaic, error: Error = '', message: str = ''):
        self.mosaic = mosaic
        self.message = f'Erro ao processar mosaico {mosaic}.\n{message}\n{error}'
        super().__init__(self.message)

class DatabaseInsertionError(Error):
    """Logs database insertion Errors and the reasons for it, without interrupting execution"""

    def __init__(self, table: str = '', data: dict = {}, error: Error = ''):
        self.interrupt_execution = False
        self.data = data
        self.table = table
        self.message = f'Erro ao inserir dados na tabela {table}.\n{error}.\n{data}'
        super().__init__(self.message)

class InvalidPathError(Error):
    """Error manager for a full path requested to an object without a set path"""

    def __init__(self, object):
        self.object = object
        self.message = f'Não foi possível encontrar o caminho, atributo "path" não configurado.\n{object.__dict__}'
        super().__init__(self.message)

class InvalidMLClassifierError(Error):
    """Error manager for a ML classifier other then 'CPU' or 'GPU'"""

    def __init__(self, p_type: str):
        self.message = f'Não é possível utilizar {type} como método de processamento, favor informar "CPU" ou "GPU"'
        super().__init__(self.message)

class VariablesLoadingError(Error):
    """Error for logging variables that failed to load"""
    
    def __init__(self, variables):
        self.interrupt_execution = False
        self.message = f'Não foi possível carregar as variáveis:\n{variables}'
        super().__init__(self.message)

class DeletionError(Error):
    """Custom logging for Deletion Errors"""

    def __init__(self, path):
        self.interrupt_execution = False
        self.message = f'Não foi possível deletar o diretório:\n{path}'
        super().__init__(self.message)

class NoBaseTilesLayerFound(Error):
    """Error for an invalid or unexisting tiles layer"""

    def __init__(self, message = ''):
        self.message = f'Não foi possível encontrar a layer de tiles.\n{message}'
        super().__init__(self.message)