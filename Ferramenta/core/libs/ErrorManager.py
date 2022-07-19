class Error(Exception):
    """Base class for other exceptions"""

class FolderAccessError(Error):
    """Exception for an unaccessible folder"""

    def __init__(self, folder):
        self.message = f'Não foi possível acessar o diretório {folder}.'
        super().__init__(self.message)

class UnexistingFeatureError(Error):
    """Exception for layers and features that don't exist"""

    def __init__(self, feature) -> None:
        self.feature = feature
        self.message = f'Não foi possível acessar a feição {feature}.'
        super().__init__(self.message)

class UnexistingSDEConnectionError(Error):
    """Exception for SDE connection that doesn't exist"""

    def __init__(self, sde) -> None:
        self.sde = sde
        self.message = f'Não foi possível acessar a conexão SDE {sde}.'
        super().__init__(self.message)

class UnexistingGDBConnectionError(Error):
    """Exception for GDB that doesn't exist"""

    def __init__(self, gdb : str) -> None:
        self.gdb =  gdb
        self.message = f'Não foi possível acessar/criar o GDB {gdb}.'
        super().__init__(self.message)

class UnexistingFeatureDatasetError(Error):
    """Exception for GDB that doesn't exist"""

    def __init__(self, feature_dataset : str) -> None:
        self.feature_dataset =  feature_dataset
        self.message = f'Não foi possível acessar/criar o Feature Dataset {feature_dataset}.'
        super().__init__(self.message)

class NotADatabase(Error):
    """Exception for when a feature is not inside a Database"""

    def __init__(self, path : str) -> None:
        self.database = path
        self.message = f'{path} não é em um GDB ou SDE.'
        super().__init__(self.message)

class SavingEditingSessionError(Error):
    """Handler for saving editing on a database when session is closed"""

    def __init__(self, session: str):
        self.sesison = session
        self.message = f'Não foi possível salvar as edições no banco {session}'
        super().__init__(self.message)

class MaxFailuresError(Error):
    """Handler for failed attempts that exceed a maximum limit"""
    max_failures = 40
    
    def __init__(self, method: str, attempts: int):
        self.method = method
        self.attempts = attempts
        self.message = f'O método {method} falhou {attempts} vezes consecutivas'
        super().__init__(self.message)
