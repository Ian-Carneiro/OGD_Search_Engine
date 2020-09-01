from time import time
from index_log import log
from neo4j import GraphDatabase, basic_auth
from requests import get
import _csv
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout, MissingSchema
from csv import field_size_limit, Sniffer, reader, QUOTE_ALL
from sys import maxsize
from chardet.universaldetector import UniversalDetector

from model import MetadataDataset, MetadataResources
from spatial_indexing.neo4j_index import return_type_place, insert_into_resource_place, \
    Neo4jIndex, delete_spatial_index, update_num_resources
from neobolt.exceptions import CypherSyntaxError
from statistics import mode, StatisticsError
from multiprocessing import Process, Manager, Lock, Value
from os import cpu_count, remove, path

# precisa estar encrypted=False quando rodar local
driver = GraphDatabase.driver("bolt://0.0.0.0:7687", auth=basic_auth("neo4j", "neo4j"), encrypted=False)

field_size_limit(maxsize)


def types_and_indexes(csv_file: list, driver_: GraphDatabase):
    """
        Existem colunas com, por exemplo, nomes de bairros que são também nomes de municípios, portanto este método busca
    diminuir a identificação de colunas de “falsos” locais, verificando uma certa quantidade de
    linhas do CSV e selecionando os tipos de locais(Municípios, UFs, Regiões) que mais apareceram, bem como os índices
    que indicam onde estão os locais encontrados.
    """
    quant_rows = 0
    quant_none_place = 0
    quant_rows_with_place_found = 0
    list_patterns = []
    for row in csv_file:
        quant_rows += 1
        pattern_type = ""
        pattern_index = ""
        undefined_type = False
        for j in range(len(row)):
            res = None
            try:
                res = return_type_place(row[j], driver_)
            except CypherSyntaxError as err:
                log.info(
                    "Erro ao verificar se o termo é um local e se possui um tipo.(índice a partir do 0) i={0}, j={1}"
                        .format(str(quant_rows), str(j)))
            if res:
                if res != "UNDEFINED":
                    pattern_type = "+".join([pattern_type, res])
                    pattern_index = "+".join([pattern_index, str(j)])
                else:
                    undefined_type = True
                    break
        if pattern_type:
            quant_rows_with_place_found += 1
        if pattern_type and not undefined_type:
            list_patterns.append("|".join([pattern_type[1:], pattern_index[1:]]))
        else:
            quant_none_place += 1
    if list_patterns and quant_rows_with_place_found > quant_none_place:
        try:
            m = mode(list_patterns)
        except StatisticsError as err:
            m = list_patterns[0]
        types_and_index = m.split("|")
        types_in_order = types_and_index[0].split("+")
        index_cols = types_and_index[1].split("+")
        log.info("Tipos de lugares e índices encontrados: " + m)
        return types_in_order, [int(i) for i in index_cols]
    else:
        return [], []


def count_places_id(index_cols, types_in_order, len_row, csv_file, places_id_dict, start_csv, lock, quant_indexed_rows):
    quant_row = 0
    not_found_place = 0
    quant_indexed_rows_aux = 0
    places_id_dict_aux = {}
    if index_cols:
        neo4j_index = Neo4jIndex()
        for row in csv_file:
            quant_row += 1
            if len(row) == len_row:
                try:
                    res = neo4j_index.find_places_return_id([row[i] for i in index_cols], types_in_order)
                except CypherSyntaxError:
                    res = None
                    log.info(f"CypherSyntaxError. Erro na linha {quant_row}")
                if not res and not_found_place < 2:
                    not_found_place += 1
                    log.info(f"Não foi encontrado 'locais' na linha {quant_row + start_csv}")
                elif res:
                    quant_indexed_rows_aux += 1
                    for id_ in res:
                        try:
                            places_id_dict_aux[id_] += 1
                        except KeyError:
                            places_id_dict_aux[id_] = 1
            else:
                log.info(f"Linha do CSV não corresponde ao tamanho encontrado. Num. linha: {quant_row + start_csv}")

        lock.acquire()
        quant_indexed_rows.value += quant_indexed_rows_aux
        for key in places_id_dict_aux.keys():
            try:
                places_id_dict[key] += places_id_dict_aux[key]
            except KeyError:
                places_id_dict[key] = places_id_dict_aux[key]
        lock.release()
    del csv_file


def read_lines_file(file, num_lines):
    return [file.readline() for i in range(num_lines)]


def analyze_csv(file):
    try:
        dialect = Sniffer().sniff(''.join(read_lines_file(file, 50)))
        log.info(f"delimiter: ({dialect.delimiter}) doublequote: ({dialect.doublequote}) "
                 f"escapechar: ({dialect.escapechar}) "
                 f"lineterminator: ({dialect.lineterminator}) quotechar: ({dialect.quotechar}) "
                 f"quoting: ({dialect.quoting}) "
                 f"skipinitialspace: ({dialect.skipinitialspace})")
    except _csv.Error:
        log.info("Não foi possível determinar o delimitador.")
        return ()
    file.seek(0)
    csv_file = reader(read_lines_file(file, 100), dialect, quoting=QUOTE_ALL)
    csv_file = list(csv_file)
    # Verifica qual o provável tamanho de cada linha do csv
    try:
        len_row = mode([len(x) for x in csv_file[0:]])
        log.info(f"tamanho provável da linha: {len_row}")
    except StatisticsError:
        log.info('Problema ao verifica qual o provável tamanho de cada linha do csv')
        return ()
    time_i = time()
    types_and_indexes_ = types_and_indexes(csv_file, driver)
    time_f = time()
    log.info(f"Tempo para verificação de tipos: {time_f - time_i}")
    if not types_and_indexes_[0]:
        return ()
    file.seek(0)
    return dialect, len_row, types_and_indexes_


def index(resource: MetadataResources, num_package_resources, encoding, quant_process=cpu_count()):
    try:
        with open('./spatial_indexing/tmp_csv.csv', 'r', encoding=encoding, newline=None) as file_contents:
            res = analyze_csv(file_contents)
            if not res:
                return
            dialect = res[0]
            len_row = res[1]
            types_and_indexes_ = res[2]

            with Manager() as manager:
                places_id_dict = manager.dict()
                quant_indexed_rows = Value('d', 0.0)
                lock = Lock()
                time_i = time()
                while True:
                    csv_file = reader(file_contents.readlines(1024 * 1024 * 50), dialect, quoting=QUOTE_ALL)
                    csv_file = list(csv_file)
                    if not csv_file:
                        break
                    # -----------------------------------------------------------------------------------------#
                    types_in_order = types_and_indexes_[0]
                    index_cols = types_and_indexes_[1]
                    len_csv_file = len(csv_file)
                    csv_file_division = int(len_csv_file / quant_process)
                    # -----------------------------------------------------------------------------------------#
                    if quant_indexed_rows.value > 0:
                        log.info(f"Já existiam {quant_indexed_rows.value} linhas indexadas deste resource. "
                                 f"O total de linhas indexadas será atualizado")
                    processes = []
                    for i in range(quant_process):
                        start = csv_file_division * i
                        if i != quant_process - 1:
                            end = csv_file_division * (1 + i)
                            processes.append(Process(
                                target=count_places_id,
                                args=(index_cols, types_in_order, len_row, csv_file[start:end],
                                      places_id_dict, start, lock, quant_indexed_rows)))
                        else:
                            processes.append(Process(target=count_places_id,
                                                     args=(
                                                         index_cols, types_in_order, len_row,
                                                         csv_file[start:],
                                                         places_id_dict, start, lock, quant_indexed_rows)))

                    for process in processes:
                        process.start()

                    for process in processes:
                        process.join()

                    log.info(f"{len_csv_file} linhas foram verificadas")

                if resource.updated:
                    log.info(f"recurso {resource.id} marcado para atualização")
                    delete_spatial_index(resource.id, driver)
                for key in places_id_dict:
                    try:
                        insert_into_resource_place(key, resource, num_package_resources, quant_indexed_rows.value,
                                                   places_id_dict[key], driver)
                    except CypherSyntaxError:
                        log.info(f"CypherSyntaxError. key:{key} quant_places: {places_id_dict[key]} "
                                 f"quant_indexed_rows: {quant_indexed_rows.value} "
                                 f"resource: {resource.id, resource.package_id}")
                time_f = time()
                log.info(f"Quantidades de linhas indexadas do CSV: {quant_indexed_rows.value}")
                log.info(f"Tempo de indexação(em segundos): {time_f - time_i}")
                log.info(
                    f"Tempo médio de indexação de linha do CSV(em segundos): "
                    f"{(time_f - time_i) / quant_indexed_rows.value}\n")
    except UnicodeError or _csv.Error as err:
        # encoding = 'ISO-8859-1'
        log.info('Ao decodificar o arquivo, uma exceção ocorreu...')
        log.info(err)


def run_spatial_dataset_indexing(dataset: MetadataDataset, update_num_package_resources):
    detector_charset = UniversalDetector()
    indexed_resources_ids = []
    if update_num_package_resources:
        update_num_resources(dataset.id, dataset.num_resources, driver)
    for resource in dataset.resources:
        if path.exists("./spatial_indexing/tmp_csv.csv"):
            remove("./spatial_indexing/tmp_csv.csv")
        log.info('id_resource: ' + resource.id + ' url: ' + resource.url)
        try:
            with get(resource.url, timeout=(5, 36), stream=True) as request:
                encoding = request.encoding
                log.info('headers: ' + request.headers.__str__())
                if not request.headers['Content-Type'].__contains__('text/html') and \
                        not request.headers['Content-Type'].__contains__('text/css') and \
                        not request.headers['Content-Type'].__contains__('text/xml') and \
                        not request.headers['Content-Type'].__contains__('application/vnd.ms-excel') and \
                        not request.headers['Content-Type'].__contains__(
                            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):
                    with open('./spatial_indexing/tmp_csv.csv', 'wb') as file_contents:
                        detector_charset.reset()
                        for chunk in request.iter_content(chunk_size=1024 * 1024 * 50):
                            file_contents.write(chunk)
                            detector_charset.feed(chunk)
                        detector_charset.close()
                    log.info(f'charset para csv encontrado na requisição: {encoding}')
                    log.info(f"charset para csv encontrado em verificação[charset: confiança(max. 1.0)]: "
                             f"{detector_charset.result['encoding']}: {detector_charset.result['confidence']}")
                    if not encoding or detector_charset.result['confidence'] >= 0.9:
                        encoding = detector_charset.result['encoding']
                    index(resource, dataset.num_resources, encoding)
                    indexed_resources_ids.append(resource.id)
        except ConnectionError:
            log.info('Erro de Conexão, ConnectionError')
        except ReadTimeout:
            log.info('Erro de Conexão, ReadTimeout')
        except MissingSchema:
            log.exception('requests.exceptions.MissingSchema', exc_info=True)
        except ChunkedEncodingError:
            log.info('a conexação foi encerrada, ChunkedEncodingError')
        except StopIteration:
            log.info('Chunk vazio')
    return indexed_resources_ids


def delete_spatial_indexes(dataset: MetadataDataset):
    for resource in dataset.resources:
        delete_spatial_index(resource.id, driver)
    update_num_resources(dataset.id, dataset.num_resources, driver)
