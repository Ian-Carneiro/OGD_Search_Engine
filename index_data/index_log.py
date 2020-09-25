import logging
from logging.handlers import RotatingFileHandler
from config import config

handler = RotatingFileHandler(config.log["file_name"], maxBytes=config.log["max_file_bytes"],
                              backupCount=config.log["backup_count"])
logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO, handlers=[handler])

log = logging.getLogger()
