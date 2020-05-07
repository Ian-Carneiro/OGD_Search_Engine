# import metadata_processor.metadata_processor
from requests import get
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout, MissingSchema
from io import StringIO
import _csv
from csv import field_size_limit, Sniffer, reader, QUOTE_ALL
from sys import maxsize
from statistics import mode, StatisticsError
from neobolt.exceptions import CypherSyntaxError
from index_log import log
from model import get_resources
from spatial_indexing.spatial_indexing import run_spacial_indexing


resources = get_resources()
quant = 556
resources = resources[quant:]

field_size_limit(maxsize)
for resource in resources:
    log.info("------------------------------------------------------------------------------------------------------")
    log.info(str(quant) + ' - ' + resource.id + "  " + resource.url)
    try:
        request = get(resource.url, timeout=(30, 3600))  # , stream=True)
        log.info('headers: ' + request.headers.__str__())
        if not request.headers['Content-Type'].__contains__('text/html') and \
                not request.headers['Content-Type'].__contains__('text/css') and \
                not request.headers['Content-Type'].__contains__('text/xml') and \
                not request.headers['Content-Type'].__contains__('application/vnd.ms-excel') and \
                not request.headers['Content-Type'].__contains__(
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):

            if len(request.headers['Content-Type'].split('charset=')) == 1 and \
                    not request.headers['Content-Type'].__contains__('application/octet-stream'):
                file_contents = StringIO(request.content.decode('utf-8'), newline=None)
            else:
                file_contents = StringIO(request.text, newline=None)

    except _csv.Error as err:
        file_contents = StringIO(request.content.decode('utf-16'), newline=None)
        log.info('Ao decodificar o arquivo, uma exceção ocorreu... mudando para UTF-16')
    except ConnectionError as err:
        log.info('Erro de Conexão, ConnectionError')
        with open('./logs/errors/ConnectionError.txt', 'a') as connection_error:
            connection_error.write(resource.id)
    except ReadTimeout as err:
        log.info('Erro de Conexão, ReadTimeout')
        with open('./logs/errors/ReadTimeout.txt', 'a') as read_timeout:
            read_timeout.write(resource.id)
    except UnicodeError as err:
        file_contents = StringIO(request.content.decode('ISO-8859-1'), newline=None)
        log.info('Ao decodificar o arquivo, uma exceção ocorreu... mudando para ISO-8859-1')
    except MissingSchema as err:
        log.exception('requests.exceptions.MissingSchema', exc_info=True)
        with open('./logs/errors/MissingSchema.txt', 'a') as missing_schema:
            missing_schema.write(resource.id)
    except ChunkedEncodingError as err:
        log.info('a conexação foi encerrada, ChunkedEncodingError')
        with open('./logs/errors/ChunkedEncodingError.txt', 'a') as chunked_encoding_error:
            chunked_encoding_error.write(resource.id)

    if 'file_contents' in locals():
        try:
            dialect = Sniffer().sniff(''.join(file_contents.readlines(5)))
            file_contents.seek(0)
            csv_file = reader(file_contents, dialect, quoting=QUOTE_ALL)
            csv_file = list(csv_file)

            # Verifica qual o provável tamanho de cada linha do csv
            len_row = mode([len(x) for x in csv_file[0:1024]])

            run_spacial_indexing(csv_file, len_row, resource)

        except CypherSyntaxError as err:
            log.exception("CypherSyntaxError", exc_info=True)
        except StatisticsError as err:
            log.exception("StatisticsError", exc_info=True)
        del file_contents
    quant += 1
    # break

