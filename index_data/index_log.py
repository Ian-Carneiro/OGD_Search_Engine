import logging
from logging.handlers import RotatingFileHandler
from config import config
import os

if not os.path.exists("./logs/data_processor.log"):
    os.mkdir("./logs")
    os.mknod("./logs/data_processor.log")

handler = RotatingFileHandler(config.log["file_name"], maxBytes=config.log["max_file_bytes"],
                              backupCount=config.log["backup_count"])
logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO, handlers=[handler])

log = logging.getLogger()
