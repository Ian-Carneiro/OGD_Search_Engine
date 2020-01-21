# import metadata_processor
import requests
from sqlalchemy import create_engine, func
from sqlalchemy.exc import DataError
import io
import _csv
import csv
import sys
import psycopg2
import re
from statistics import mode
# import pandas
# import zipfile
# import xlrd

# metadata_processor.run()
# engine = create_engine('postgresql+psycopg2://postgres:postgres@db_postgres_tcc:5432/data_processor_db')
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/data_processor_db')
sql = "select url, id from metadata_resources " \
      "where format ilike 'csv' and url ilike 'http%%://%%' and url not ilike '%%.zip' order by created asc"
resources_urls = engine.execute(sql).fetchall()
resources_urls = resources_urls[30:]

# """
quant = 30
two_space_sep = []
urls_timeout = []
urls_invalid_header_content_type = []
chunksize = 500000
csv.field_size_limit(sys.maxsize)
for url in resources_urls:
    print('--------------------------------------------------------------------------------------------------------')
    print(quant, ' : ', url)
    try:
        request = requests.get(url[0], timeout=(30, 3600))#, stream=True)
        print('headers: ', request.headers)
        if not request.headers['Content-Type'].__contains__('text/html') and \
                not request.headers['Content-Type'].__contains__('text/css') and \
                not request.headers['Content-Type'].__contains__('text/xml') and \
                not request.headers['Content-Type'].__contains__('application/vnd.ms-excel') and \
                not request.headers['Content-Type'].__contains__(
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):

            if len(request.headers['Content-Type'].split('charset=')) == 1 and \
                    not request.headers['Content-Type'].__contains__('application/octet-stream'):
                file_contents = io.StringIO(request.content.decode('utf-8'))
            else:
                file_contents = io.StringIO(request.text)
        else:
            urls_invalid_header_content_type.append(url[0])
    except _csv.Error as err:
        print('_csv.Error:', err, ':', url)
        file_contents = io.StringIO(request.content.decode('utf-16'))
    except requests.exceptions.ConnectionError as err:
        print('requests.exceptions.ConnectionError:', err, ':', url)
        urls_timeout.append(url[0])
    except requests.exceptions.ReadTimeout as err:
        print('requests.exceptions.ReadTimeout:', err, ':', url)
        urls_timeout.append(url[0])
    except UnicodeError as err:
        print('UnicodeError:', err, ':', url)
    except requests.exceptions.MissingSchema as err:
        print('requests.exceptions.MissingSchema:', err, ':', url)
    except requests.exceptions.ChunkedEncodingError as err:
        print('requests.exceptions.ChunkedEncodingError:', err, ':', url)

# with open('./teste_csv/ponto_e_virgula.csv', 'r', encoding='utf8') as file_contents:
    if 'file_contents' in locals():
        try:
            dialect = csv.Sniffer().sniff(file_contents.read(1024))
            file_contents.seek(0)
            csv_file = csv.reader(file_contents, dialect)
            csv_file = list(csv_file)

            # Verifica qual o provável tamanho de cada linha do csv
            len_row = mode([len(x) for x in csv_file[0:1024]])

            conn = engine.raw_connection()
            cursor = conn.cursor()
            count_row = 0
            types_in_order = []
            index_cols = []
            for row in csv_file:
                count_col = 0
                count_row += 1
                types_in_order_aux = []
                index_cols_aux = []
                places_and_types_checked = True
                for data in row:
                    cursor.callproc("return_type_place", [data])
                    res = cursor.fetchone()
                    conn.commit()
                    if res[0] and res[0] != "UNDEFINED":
                        types_in_order_aux.append(res[0])
                        index_cols_aux.append(count_col)
                    elif res[0] == "UNDEFINED":
                        places_and_types_checked = False
                        break
                    count_col += 1
                if len(types_in_order_aux) > len(types_in_order) and places_and_types_checked:
                    types_in_order = types_in_order_aux.copy()
                    index_cols = index_cols_aux.copy()
                if types_in_order and count_row >= 300:
                    break
            count_row = 0
            not_found_place = 0
            print(types_in_order)
            for row in csv_file:
                count_row += 1
                if len(row) == len_row:
                    cursor.callproc("find_places_and_index", [re.sub(r'[\(\)\/\.]', '', '|'.join(row)),
                                                              url[1],
                                                              [row[i] for i in index_cols],
                                                              types_in_order])
                    res = cursor.fetchone()
                    conn.commit()
                    if not res[0] and not_found_place <= 10:
                        not_found_place += 1
                        print(count_row, res)
                else:
                    print(count_row)
            cursor.close()
            conn.close()
            del csv_file
        except _csv.Error as err:
            print('_csv.Error 2:', err, ':', url)
        except DataError as err:
            print('sqlalchemy.exc.DataError:', err, ':', url)
        except TypeError as err:
            print('TypeError:', err, ':', url)
        # except psycopg2.errors.InvalidRegularExpression as err:
        #     print('InvalidRegularExpression:', err, ':', url)
    quant += 1
    print('--------------------------------------------------------------------------------------------------------')
print('urls => requests timeout: \n', urls_timeout, 'quant urls: ', len(urls_timeout))
print('urls => csv sep = \s+: \n', two_space_sep, 'quant urls: ', len(two_space_sep))
print('urls => invalid header content type: \n', urls_invalid_header_content_type, 'quant urls: ',
      len(urls_invalid_header_content_type))
# """









# with open('./teste_csv/CON01.csv', 'r', encoding='utf8') as f:
#     mycsv = csv.reader(f)
#     for l in list(mycsv):
#         print('|'.join(l))
#         res = engine.execute("select find_and_index_places('{}', '{}');".format('|'.join(l))).fetchone()
#         print(res)

# with open('./teste_csv/ponto_e_virgula.csv', 'r', encoding='utf8') as f:
#     csv_file = csv.reader(f)
#     conn = engine.raw_connection()
#     cursor = conn.cursor()
#     cursor.callproc("find_places_and_index_csv", [list(csv_file)])
#     res = cursor.fetchone()
#     conn.commit()
#     cursor.close()
#     conn.close()
#     print(res[0])

# with open('./teste_csv/ponto_e_virgula.csv', 'r', encoding='utf8') as f:
#     dialect = csv.Sniffer().sniff(f.read(1024))
#     f.seek(0)
#     csv_file = csv.reader(f, dialect)
#
#     count_row = 0
#     conn = engine.raw_connection()
#     cursor = conn.cursor()
#     for row in list(csv_file):
#         count_row += 1
#         places_in_order = []
#         types_in_order = []
#         places_and_types_checked = True
#         for data in row:
#             cursor.callproc("return_type_place", [data])
#             res = cursor.fetchone()
#             conn.commit()
#             if res[0] is not None and res[0] != "UNDEFINED":
#                 places_in_order.append(data)
#                 types_in_order.append(res[0])
#             elif res[0] == "UNDEFINED":
#                 places_and_types_checked = False
#                 break
#         if (places_and_types_checked and places_in_order) or count_row == 2:
#             break
#     print(places_in_order)
#     print(types_in_order)
#     print(count_row)


"""
quant = 30
two_space_sep = []
urls_timeout = []
urls_invalid_header_content_type = []
chunksize = 500000
csv.field_size_limit(sys.maxsize)
for url in resources_urls:
    print('--------------------------------------------------------------------------------------------------------')
    print(quant, ' : ', url)
    try:
        request = requests.get(url[0], timeout=(30, 3600))#, stream=True)
        print('headers: ', request.headers)
        if not request.headers['Content-Type'].__contains__('text/html') and \
                not request.headers['Content-Type'].__contains__('text/css') and \
                not request.headers['Content-Type'].__contains__('text/xml') and \
                not request.headers['Content-Type'].__contains__('application/vnd.ms-excel') and \
                not request.headers['Content-Type'].__contains__(
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):

            if len(request.headers['Content-Type'].split('charset=')) == 1 and \
                    not request.headers['Content-Type'].__contains__('application/octet-stream'):
                file_contents = io.StringIO(request.content.decode('utf-8'))
            else:
                file_contents = io.StringIO(request.text)
        else:
            urls_invalid_header_content_type.append(url[0])
    except _csv.Error as err:
        print('_csv.Error:', err, ':', url)
        file_contents = io.StringIO(request.content.decode('utf-16'))
    except requests.exceptions.ConnectionError as err:
        print('requests.exceptions.ConnectionError:', err, ':', url)
        urls_timeout.append(url[0])
    except requests.exceptions.ReadTimeout as err:
        print('requests.exceptions.ReadTimeout:', err, ':', url)
        urls_timeout.append(url[0])
    except UnicodeError as err:
        print('UnicodeError:', err, ':', url)
    except requests.exceptions.MissingSchema as err:
        print('requests.exceptions.MissingSchema:', err, ':', url)
    except requests.exceptions.ChunkedEncodingError as err:
        print('requests.exceptions.ChunkedEncodingError:', err, ':', url)

    if 'file_contents' in locals():
        try:
            dialect = csv.Sniffer().sniff(file_contents.read(1024))
            file_contents.seek(0)
            csv_file = csv.reader(file_contents, dialect)

            count_row = 0
            conn = engine.raw_connection()
            cursor = conn.cursor()
            #Lembrar de verificar várias linhas para ter certeza
            #Lembrar de guardar o indice das colunas que possuem valores com a ocorrência
            for row in list(csv_file):
                count_row += 1
                types_in_order = []
                places_and_types_checked = True
                for data in row:
                    cursor.callproc("return_type_place", [data])
                    res = cursor.fetchone()
                    conn.commit()
                    if res[0] and res[0] != "UNDEFINED":
                        types_in_order.append(res[0])
                    elif res[0] == "UNDEFINED":
                        places_and_types_checked = False
                        break
                if (places_and_types_checked and types_in_order) or count_row == 300:
                    break
            count_row = 0
            not_found_place = 0
            print(types_in_order)
            file_contents.seek(0)
            csv_file = csv.reader(file_contents, dialect)
            for row in list(csv_file):
                places_in_order = []
                for data in row:
                    cursor.callproc("return_if_place", [data])
                    res = cursor.fetchone()
                    conn.commit()
                    if res[0]:
                        places_in_order.append(data)
                cursor.callproc("find_places_and_index", [('|'.join(row))
                                .replace('(', '').replace(')', '').replace(r'.', '').replace('[', '').replace(']', ''),
                                                          url[1],
                                                          places_in_order, types_in_order])
                res = cursor.fetchone()
                conn.commit()
                count_row += 1
                if not res[0] and not_found_place <= 10:
                    not_found_place += 1
                    print(count_row, res, end=' ')
            cursor.close()
            conn.close()
            del csv_file
        except _csv.Error as err:
            print('_csv.Error 2:', err, ':', url)
        except DataError as err:
            print('sqlalchemy.exc.DataError:', err, ':', url)
        except TypeError as err:
            print('TypeError:', err, ':', url)
        except psycopg2.errors.InvalidRegularExpression as err:
            print('TypeError:', err, ':', url)
    quant += 1
    print('--------------------------------------------------------------------------------------------------------')
print('urls => requests timeout: \n', urls_timeout, 'quant urls: ', len(urls_timeout))
print('urls => csv sep = \s+: \n', two_space_sep, 'quant urls: ', len(two_space_sep))
print('urls => invalid header content type: \n', urls_invalid_header_content_type, 'quant urls: ',
      len(urls_invalid_header_content_type))
"""



# a = zipfile.ZipFile('./TesteCSV/ponto_e_virgula.csv.zip').read('ponto_e_virgula.csv')
# df = pandas.read_csv(io.StringIO(a.decode('ISO-8859-1')), sep=None, engine='python')
# print(df)


# if request.headers['Content-Type'] != 'text/csv':
#     csv = zipfile.ZipFile(io.BytesIO(request.content))


# csv = requests.get('http://www.anp.gov.br/images/dadosabertos/precos/2016-1_GLP.csv').content.decode('ISO-8859-1')
# csv = io.StringIO(csv)
# print(csv.readline().count('  '))

# df = pandas.read_csv('./TesteCSV/ponto_e_virgula.csv', sep=None, engine='python', skip_blank_lines=True, chunksize=1)
#
# for chunk in df:
#     for key, value in chunk.iterrows():
#         print(key, value)
        # engine.execute("select nome from uf where upper(nome) similar to {}".format('|'.join(value)))



# print(df)


# url = r'http://dados.df.gov.br/pt_BR/dataset/f02045ac-4db9-4536-b8a5-651da3e96414/resource/7e40e21d-c5ad-4182-be68-deaad4a75980/download/demonstrativo-de-despesas-com-diarias-e-passagens-de-2016-e-2017.xlsx'
# r = requests.get(url)
# csv_file = io.StringIO(r.text)
# df = pandas.read_excel(csv_file)
# print(df)

"""
espaco = pandas.read_csv('./TesteCSV/espaço.csv', sep='\s+', engine='python')
espaco_mais = pandas.read_csv('./TesteCSV/espaço+.csv', sep=None, engine='python', skip_blank_lines=True, skipinitialspace=True)
# compression = infer(default) |
ponto_e_virgula = pandas.read_csv('./TesteCSV/ponto_e_virgula.csv', sep=None, engine='python')
tab = pandas.read_csv('./TesteCSV/tab.csv', sep=None, engine='python')
virgula = pandas.read_csv('./TesteCSV/virgula.csv', sep=None, engine='python')
mal_formatado_1 = pandas.read_csv('./TesteCSV/mal_formatado_1.csv', sep=None, engine='python', error_bad_lines=False)
mal_formatado_2 = pandas.read_csv('./TesteCSV/mal_formatado_2.csv', sep=None, engine='python', error_bad_lines=False)
mal_formatado_3 = pandas.read_csv('./TesteCSV/mal_formatado_3.csv', sep=None, engine='python', error_bad_lines=False)

print('espaco:\n', espaco)
print('espaco+:\n', espaco_mais)
print('ponto_e_virgula:\n', ponto_e_virgula)
print('tab:\n', tab)
print('virgula:\n', virgula)
print('mal_formatado_1:\n', mal_formatado_1)
print('mal_formatado_2:\n', mal_formatado_2)
print('mal_formatado_3:\n', mal_formatado_3)
"""
