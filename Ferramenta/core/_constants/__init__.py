import os

CONSTANTS_DIR = os.path.dirname(__file__)
CORE_DIR = os.path.dirname(CONSTANTS_DIR)

ADAPTERS_DIR = os.path.join(CORE_DIR, 'adapters')
CONFIGS_DIR = os.path.join(CORE_DIR, 'configs')
INSTANCES_DIR = os.path.join(CORE_DIR, 'instances')
LIBS_DIR = os.path.join(CORE_DIR, 'libs')
ML_MODELS_DIR = os.path.join(CORE_DIR, 'ml_models')
ADAPTEES_DIR = os.path.join(CORE_DIR, 'adaptees')
IMAGERY_SERVICES_DIR = os.path.join(ADAPTEES_DIR, 'SateliteImagery')
LOGS_DIR = os.path.join(CORE_DIR, 'logs')

ROOT_DIR = os.path.dirname(CORE_DIR)
DOWNLOADS_DIR = os.path.join(os.path.dirname(ROOT_DIR), 'downloads')
TEMP_DIR = os.path.join(os.path.dirname(ROOT_DIR), 'temp')
TEMP_DB = f'{TEMP_DIR}.gdb'
# TEMP_DB = 'IN_MEMORY'