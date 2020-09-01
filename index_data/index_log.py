import logging
from logging.handlers import RotatingFileHandler

handler = RotatingFileHandler('./logs/data_processor.log', maxBytes=1024*1024, backupCount=0)

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO, handlers=[handler])

log = logging.getLogger()
