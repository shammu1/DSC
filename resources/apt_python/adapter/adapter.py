import json
import logging
#from ..Package.logger import dfl_logger as Logger

class ResourceAdapter:
    @staticmethod
    def log_error(message: str, target: str = None, **kwargs):
        Logger.error(message, target, **kwargs)

    @staticmethod
    def log_info(message: str, target: str = None, **kwargs):
        Logger.info(message, target, **kwargs)
