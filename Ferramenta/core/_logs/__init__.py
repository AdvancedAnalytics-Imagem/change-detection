import logging
from datetime import datetime
from arcpy import AddError, AddMessage, AddWarning, SetProgressor
from core._constants import *


class LogLevels:
    INFO = 'info'
    ERROR = 'error'
    WARNING = 'warning'
    DEBUG = 'debug'
    CRITICAL = 'critical'

class MessageLogging:
    def __init__(self, *args, **kwargs):
        logging.basicConfig(
            filename=os.path.join(LOGS_DIR, f'general_loggin_{datetime.now().strftime("%Y%m%d%H%M%S")}.log'),
            filemode='w',
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%d-%b-%y %H:%M:%S',
            level=logging.INFO
        )

    def info(self, message: str, display_message: bool):
        if display_message:
            AddMessage(message=message)
        logging.info(message)

    def debug(self, message: str, display_message: bool):
        if display_message:
            AddWarning(message=f'Debug - {message}')
        logging.debug(message)
    
    def warning(self, message: str, display_message: bool):
        if display_message:
            AddWarning(message=message)
        logging.warning(message)

    def error(self, message: str, display_message: bool):
        if display_message:
            AddError(message=message)
        logging.error(message)

    def critical(self, message: str, display_message: bool):
        if display_message:
            AddError(message=f'Critical Error - {message}')
        logging.critical(message)
    
    def progress(self, message: str):
        SetProgressor(type="default", message=message)


log_message = MessageLogging()

def aprint(message: str, level: LogLevels = None, display_message: bool = True, progress: bool = False):
    message = u'{}'.format(message)

    if not level or level is LogLevels.INFO:
        log_message.info(message, display_message=display_message)

    if level is LogLevels.ERROR:
        log_message.error(message, display_message=display_message)

    if level is LogLevels.WARNING:
        log_message.warning(message, display_message=display_message)
        
    if level is LogLevels.DEBUG:
        log_message.debug(message, display_message=display_message)

    if level is LogLevels.CRITICAL:
        log_message.critical(message, display_message=display_message)
    
    if progress:
        log_message.progress(message)
