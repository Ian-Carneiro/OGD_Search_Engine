from dateparser.search import search_dates
import datetime
import dateparser.conf
import re
from index_log import log
from temporal_indexing.config import config


def search(text):
    if not text or len(text) == 0:
        return []
    text = text.upper()
    combination_one = re.findall(r'\b(?:(?:\d{1,2}\/\d{1,2}\/(?:\d{4}|\d{2}))|(?:\d{1,2}-\d{1,2}-(?:\d{4}|\d{2}))|'
                                 r'(?:\d{1,2}(?:\/|-)\d{4}))', text)

    # --------------------------------------------------------------------------------------------------------------

    combination_two = re.findall(r'(?:(?:(?:\d{1,2}º?)|(?:primeiro))\s?(?:de|em|\/)?\s?)?'
                                 r'(?:janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro'
                                 r'|dezembro|(?:(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)\.?))\s?'
                                 r'(?:de|em|\/)?\s?(?:\d{4}|\d{2})',
                                 text, re.I)
    found = combination_one + combination_two
    if combination_two:
        combination_two = [re.sub(r'\s?(?:/|º|\.)\s?', ' ', c) for c in combination_two]
        combination_two = [re.sub(r'PRIMEIRO', '1', c) for c in combination_two]

    # --------------------------------------------------------------------------------------------------------------

    combination_three = re.findall(
        r'(?:décimo segundo|décimo primeiro|primeiro|segundo|terceiro|quarto|quinto|sexto|sétimo|oitavo'
        r'|nono|décimo|último|(?:1|2|3|4|5|6|7|8|9|10|11|12)(?:º)?)\s'
        r'(?:semestre|sem\.|bimestre|bim\.|trimestre|trim\.|mês)\s?(?:de|em|\/|\\|do ano de|do ano)\s?'
        r'(?:\d{4}|\d{2})', text, re.I)
    found += combination_three
    if combination_three:
        combination_three = [re.sub(r'\s?(?:º|\/|\\|\.)\s?', ' ', c)
                             for c in combination_three]
        combination_three = [re.sub(r'(?:\sDE\s|\sDO ANO DE\s|\sDO ANO\s|\sEM\s)', ' ', c)
                             for c in combination_three]
        combination_three = [c.split(' ') for c in combination_three]

        num = {
            'PRIMEIRO': '1',
            'SEGUNDO': '2',
            'TERCEIRO': '3',
            'QUARTO': '4',
            'QUINTO': '5',
            'SEXTO': '6',
            'SÉTIMO': '7',
            'OITAVO': '8',
            'NONO': '9',
            'DÉCIMO': '10',
            'DÉCIMO PRIMEIRO': '11',
            'DÉCIMO SEGUNDO': '12'
        }

        mes = {
            'SEM': 6,
            'SEMESTRE': 6,
            'TRIM': 3,
            'TRIMESTRE': 3,
            'BIM': 2,
            'BIMESTRE': 2,
            'MÊS': 1
        }

        combination_three_aux = []

        for c in combination_three:
            if c[0] == 'ÚLTIMO':
                date_ = f'{(int(12 / mes[c[1]] - 1) * mes[c[1]]) + 1}/{c[2]}', mes[c[1]]
            elif not c[0].isdigit():
                if len(c) == 4:
                    c[0] = c[0]+' '+c[1]
                    c.pop(1)
                date_ = f'{((int(num[c[0]]) - 1) * mes[c[1]]) + 1}/{c[2]}', mes[c[1]]
            else:
                date_ = f'{(int(c[0]) - 1) * mes[c[1]] + 1}/{c[2]}', mes[c[1]]
            combination_three_aux.append(date_)
        combination_three = combination_three_aux

    # ---------------------------------------------------------------------------------------------------------------

    text = re.sub(r'|'.join(found), '', text)
    combination_four = re.findall(r'\b\d{4}\b', text)
    # if combination_four:
        # combination_four = [c for c in combination_four]

    match = [(c, 0) if re.findall(r'(?:\w+º?)\s?(?:de|\/|\\|-|em)\s?(?:\w+)\.?\s?(?:de|\/|\\|-|em)\s?(?:\d{4}|\d{2})',
                                  c, re.I)
             else (c, 1)
             for c in combination_one + combination_two] + combination_three + [(c, 12) for c in combination_four]
    return match


def date_parser(text):
    match = search(text)
    dates = []
    for m in match:
        # '62.428.073/0001-36' <<< ('0001', 12)
        try:
            found = dateparser.search.search_dates(m[0], languages=['pt'],
                                           settings={
                                               'RELATIVE_BASE': datetime.datetime(1000, 1, 1)
                                           })
        except OverflowError:
            log.info(f"date OverflowError")
        if found and config.min_date_allowed <= found[0][1] <= config.max_date_allowed:
            dates.append((found[0][1], m[1]))
    return dates

#
# while True:
#     date = input("Informe o texto: ")
#     print(date_parser(date))
