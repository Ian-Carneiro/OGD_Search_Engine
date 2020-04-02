# import metadata_processor
import requests
from sqlalchemy import create_engine
import io
import _csv
import csv
import sys
from statistics import mode, StatisticsError
import neobolt.exceptions
import logging
from spatial_indexing import index_by_space
from resource import MetadataResources, get_resources
# from time import time

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', filename='./logs/data_processor_neo4j.log',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
log = logging.getLogger()

# metadata_processor.run()
# engine = create_engine('postgresql+psycopg2://postgres:postgres@db_postgres_tcc:5432/data_processor_db')
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/data_processor_db')


sql = """select url, id, "name", description, package_id from metadata_resources 
            where format ilike 'csv' and url ilike 'http%%://%%' and url not ilike '%%.zip' order by created asc"""
resources = engine.execute(sql).fetchall()
quant = 407
resources = resources[quant:]


csv.field_size_limit(sys.maxsize)
for resource in resources:
    log.info("------------------------------------------------------------------------------------------------------")
    log.info(str(quant) + ' - ' + resource[1] + "  " + resource[0])
    try:
        request = requests.get(resource[0], timeout=(30, 3600))  # , stream=True)
        log.info('headers: ' + request.headers.__str__())
        if not request.headers['Content-Type'].__contains__('text/html') and \
                not request.headers['Content-Type'].__contains__('text/css') and \
                not request.headers['Content-Type'].__contains__('text/xml') and \
                not request.headers['Content-Type'].__contains__('application/vnd.ms-excel') and \
                not request.headers['Content-Type'].__contains__(
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):

            if len(request.headers['Content-Type'].split('charset=')) == 1 and \
                    not request.headers['Content-Type'].__contains__('application/octet-stream'):
                file_contents = io.StringIO(request.content.decode('utf-8'), newline=None)
            else:
                file_contents = io.StringIO(request.text, newline=None)

    except _csv.Error as err:
        file_contents = io.StringIO(request.content.decode('utf-16'), newline=None)
        log.info('Ao decodificar o arquivo, uma exceção ocorreu... mudando para UTF-16')
    except requests.exceptions.ConnectionError as err:
        log.info('Erro de Conexão, ConnectionError')
    except requests.exceptions.ReadTimeout as err:
        log.info('Erro de Conexão, ReadTimeout')
    except UnicodeError as err:
        file_contents = io.StringIO(request.content.decode('ISO-8859-1'), newline=None)
        log.info('Ao decodificar o arquivo, uma exceção ocorreu... mudando para ISO-8859-1')
    except requests.exceptions.MissingSchema as err:
        log.exception('requests.exceptions.MissingSchema', exc_info=True)
    except requests.exceptions.ChunkedEncodingError as err:
        log.exception('requests.exceptions.ChunkedEncodingError', exc_info=True)

    if 'file_contents' in locals():
        try:
            dialect = csv.Sniffer().sniff(file_contents.read(1024*5))
            file_contents.seek(0)
            csv_file = csv.reader(file_contents, dialect, quoting=csv.QUOTE_ALL)
            csv_file = list(csv_file)

            # Verifica qual o provável tamanho de cada linha do csv
            len_row = mode([len(x) for x in csv_file[0:1024]])

            index_by_space(csv_file, len_row, resource[1])

        except neobolt.exceptions.CypherSyntaxError as err:
            log.exception("CypherSyntaxError", exc_info=True)
        except StatisticsError as err:
            log.exception("StatisticsError", exc_info=True)
        del file_contents
    quant += 1

