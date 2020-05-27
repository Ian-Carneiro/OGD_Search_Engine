import logging
from os.path import exists
from os import makedirs


if not exists("./logs/errors"):
    makedirs('./logs/errors')

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', filename='./logs/data_processor_neo4j.log',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
log = logging.getLogger()