from time import time
import logging
from neo4j import GraphDatabase, basic_auth
from neo4j_index import return_type_place, find_places_and_index
import neobolt.exceptions
from statistics import mode, StatisticsError

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', filename='./logs/data_processor_neo4j.log',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
log = logging.getLogger()

# precisa estar encrypted=False quando rodar local
driver = GraphDatabase.driver("bolt://0.0.0.0:7687", auth=basic_auth("neo4j", "neo4j"), encrypted=False)


def types_and_indexes(csv_file: list, quant: int, driver_: GraphDatabase):
    """
        Existem colunas com, por exemplo, nomes de bairros que são também nomes de municípios, portanto este método busca
    diminuir a identificação de colunas de “falsos” locais (Municípios, UFs, Regiões), verificando uma certa quantidade de
    linhas do CSV e selecionando os tipos de locais que mais apareceram, bem como os índices que indicam onde estão os
    locais encontrados.
    """
    count_rows = 0
    places_found = False
    count_not_undefined_places = 0
    list_patterns = []
    for row in csv_file:
        count_rows += 1
        pattern_type = ""
        pattern_index = ""
        undefined_type = False
        for j in range(len(row)):
            res = None
            try:
                res = return_type_place(row[j], driver_)
            except neobolt.exceptions.CypherSyntaxError as err:
                log.info(
                    "Erro ao verificar se o termo é um local e se possui um tipo. (índice a partir do 0) i = {0}, j = {1}"
                        .format(str(count_rows), str(j)))
            if res:
                places_found = True
                if res != "UNDEFINED":
                    pattern_type = "+".join([pattern_type, res])
                    pattern_index = "+".join([pattern_index, str(j)])
                else:
                    # para casos como: ["Itaporanga", "São Paulo"] >>>> "MUNICÍPIO|0"
                    undefined_type = True
                    break

        if pattern_type and not undefined_type:
            count_not_undefined_places += 1
            list_patterns.append("|".join([pattern_type[1:], pattern_index[1:]]))
        # or (count_rows == quant and not places_found) para o caso de não achar lugares
        # places_found será true se res!=None (UNDEFINED, MUNICÍPIO, UF, REGIÃO)
        if count_not_undefined_places == quant or (count_rows == quant and not places_found):
            break
    if list_patterns:
        try:
            m = mode(list_patterns)
        except StatisticsError as err:
            m = list_patterns[0]
            log.info("Erro no calculo da moda, provavelmente exista somente um valor na lista.")
        types_and_index = m.split("|")
        types_in_order = types_and_index[0].split("+")
        index_cols = types_and_index[1].split("+")
        log.info("Tipos de lugares e índices encontrados: " + m)
        return types_in_order, [int(i) for i in index_cols]
    else:
        return [], []


def index_by_space(csv_file, len_row, id_resource):
    time_i = time()
    # try:
    types_and_indexes_ = types_and_indexes(csv_file, 100, driver)
    types_in_order = types_and_indexes_[0]
    index_cols = types_and_indexes_[1]

    count_row = 0
    not_found_place = 0
    if index_cols:
        for row in csv_file:
            count_row += 1
            if len(row) == len_row:
                res = find_places_and_index(id_resource, [row[i] for i in index_cols], types_in_order, driver)
                if not res and not_found_place < 10:
                    not_found_place += 1
                    log.info("Não foi encontrado 'locais' na linha " + str(count_row))
            else:
                log.info("Linha do CSV não corresponde ao tamanho encontrado. Num. linha: " + str(count_row))
        log.info("Quantidades de linha do CSV: " + str(count_row))
    del csv_file
    time_f = time()
    log.info("Tempo de indexação(em segundos): " + str(time_f - time_i))
