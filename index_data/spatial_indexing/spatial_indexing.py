from time import time
from index_log import log
from neo4j import GraphDatabase, basic_auth
from spatial_indexing.neo4j_index import return_type_place, insert_into_resource_place, Neo4jIndex
from neobolt.exceptions import CypherSyntaxError
from statistics import mode, StatisticsError
from multiprocessing import Process, Manager, Lock, Value
from os import cpu_count


# precisa estar encrypted=False quando rodar local
driver = GraphDatabase.driver("bolt://0.0.0.0:7687", auth=basic_auth("neo4j", "neo4j"), encrypted=False)


def types_and_indexes(csv_file: list, quant: int, driver_: GraphDatabase):
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
                quant_rows_with_place_found += 1
                if res != "UNDEFINED":
                    pattern_type = "+".join([pattern_type, res])
                    pattern_index = "+".join([pattern_index, str(j)])
                else:
                    undefined_type = True
                    break

        if pattern_type and not undefined_type:
            list_patterns.append("|".join([pattern_type[1:], pattern_index[1:]]))
        elif not pattern_type:
            quant_none_place += 1

        if quant_rows == quant:
            break
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
                if not res and not_found_place < 10:
                    not_found_place += 1
                    log.info(f"Não foi encontrado 'locais' na linha {quant_row+start_csv}")
                elif res:
                    quant_indexed_rows_aux += 1
                    for id_ in res:
                        try:
                            places_id_dict_aux[id_] += 1
                        except KeyError:
                            places_id_dict_aux[id_] = 1
            else:
                log.info(f"Linha do CSV não corresponde ao tamanho encontrado. Num. linha: {quant_row+start_csv}")

        lock.acquire()
        quant_indexed_rows.value += quant_indexed_rows_aux
        for key in places_id_dict_aux.keys():
            try:
                places_id_dict[key] += places_id_dict_aux[key]
            except KeyError:
                places_id_dict[key] = places_id_dict_aux[key]
        lock.release()
    del csv_file


def run_spacial_indexing(csv_file, len_row, resource, quant_process=cpu_count()):
    time_i = time()
    types_and_indexes_ = types_and_indexes(csv_file, 100, driver)
    time_f = time()
    log.info(f"Tempo para verificação de tipos: {time_f-time_i}")

    if types_and_indexes_[0]:
        time_i = time()
        types_in_order = types_and_indexes_[0]
        index_cols = types_and_indexes_[1]
        len_csv_file = len(csv_file)
        csv_file_division = int(len_csv_file / quant_process)
        with Manager() as manager:
            places_id_dict = manager.dict()
            quant_indexed_rows = Value('d', 0.0)
            lock = Lock()
            processes = []
            for i in range(quant_process):
                start = csv_file_division*i
                if i != quant_process-1:
                    end = csv_file_division*(1+i)
                    processes.append(Process(
                        target=count_places_id, args=(index_cols, types_in_order, len_row, csv_file[start:end],
                                                      places_id_dict, start, lock, quant_indexed_rows)))
                else:
                    processes.append(Process(target=count_places_id,
                                             args=(index_cols, types_in_order, len_row, csv_file[start:],
                                                   places_id_dict, start, lock, quant_indexed_rows)))

            for process in processes:
                process.start()

            for process in processes:
                process.join()

            for key in places_id_dict:
                insert_into_resource_place(key, resource, quant_indexed_rows.value, places_id_dict[key], driver)
            time_f = time()
            log.info(f"Quantidades de linha do CSV: {len_csv_file}")
            log.info(f"Quantidades de linhas indexadas do CSV: {quant_indexed_rows.value}")
            log.info(f"Tempo de indexação(em segundos): {time_f-time_i}")
            log.info(f"Tempo médio de indexação de linha do CSV(em segundos): {(time_f-time_i)/quant_indexed_rows.value}")

