import os

CONSTANTS_DIR = os.path.dirname(__file__)
CORE_DIR = os.path.dirname(CONSTANTS_DIR)

ADAPTERS_DIR = os.path.join(CORE_DIR, 'adapters')
CONFIGS_DIR = os.path.join(CORE_DIR, 'configs')
INSTANCES_DIR = os.path.join(CORE_DIR, 'instances')
LIBS_DIR = os.path.join(CORE_DIR, 'libs')
ML_MODELS_DIR = os.path.join(CORE_DIR, 'ml_models')
SERVICES_DIR = os.path.join(CORE_DIR, 'services')
IMAGERY_SERVICES_DIR = os.path.join(SERVICES_DIR, 'SateliteImagery')

LOGS_DIR = os.path.join(CORE_DIR, '_logs')

ROOT_DIR = os.path.dirname(CORE_DIR)
DOWNLOADS_DIR = os.path.join(ROOT_DIR, 'downloads')
IMAGES_DIR = os.path.join(ROOT_DIR, 'images')
TEMP_DIR = os.path.join(ROOT_DIR, 'temp')