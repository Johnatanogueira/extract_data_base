import logging
import sys
import os

DEBUG = os.environ.get('DEBUG', False)

def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    
    if not logger.hasHandlers():
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        if(DEBUG):
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler(sys.stdout)
        
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
    
    return logger