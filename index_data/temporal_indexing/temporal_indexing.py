from sqlalchemy import create_engine, text
from .date_finder import date_parser
from datetime import datetime
from dateutil.relativedelta import relativedelta
from statistics import mode, StatisticsError
from index_log import log


engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5431/temporal_indexing_module')


def run_temporal_index(resource):
    funcs = [lambda: (date_parser(resource.name), f"resource.name: {resource.name}"),
             lambda: (date_parser(resource.description), f"resource.description: {resource.description}"),
             lambda: (date_parser(resource.package.title), f"resource.package.title: {resource.package.title}")]
    for func in funcs:
        dates = func()
        if dates[0]:
            break
    # -----------------------------------------------------------------------------------------------------------------
    if dates[0]:
        log.info(f"Datas em {dates[1]}")

    if len(dates[0]) == 0:
        interval = None
        # index = find_columns_with_terms(csv_file[1:102])
        # if index:
        #     interval = find_interval_in_file(csv_file, index, len_row)
        #     if interval:
        #         log.info(f"Datas no arquivo CSV nas colunas {[csv_file[0][int(i)] for i in index]}")
        if resource.created:
            date = datetime.strptime(resource.created, '%Y-%m-%dT%H:%M:%S.%f')
            interval = (date, date)
            if interval:
                log.info(f"Datas em resource.created")
    elif len(dates[0]) == 1:
        date = dates[0][0]
        interval = (date[0], date[0] + relativedelta(months=date[1] - 1, day=31)) \
            if date[1] != 0 else (date[0], date[0])
    else:
        dates = [(date[0], date[0] + relativedelta(months=date[1] - 1, day=31))
                 if date[1] != 0
                 else (date[0], date[0])
                 for date in dates[0]]
        least_recent = dates[0][0]
        last = dates[0][1]
        for date in dates[1:]:
            if date[0] < least_recent:
                least_recent = date[0]
            if date[1] > last:
                last = date[1]
        interval = [least_recent, last]

    if interval:
        interval = [i.date() for i in interval]
        with engine.connect() as conn:
            sql = text("insert into temporal_index(id_resource, interval_start, interval_end, package_id) "
                       "values(:id_resource, :interval_start, :interval_end, :package_id)")
            conn.execute(sql, id_resource=resource.id, interval_start=interval[0], interval_end=interval[1],
                         package_id=resource.package_id)
    log.info(f"intervalo encontrado: {interval}")


def find_interval_in_file(csv_file, index, len_row):
    dates = set()
    if index:
        for index_row, row in enumerate(csv_file):
            if len(row) == len_row:
                for index_col in index:
                    dates.update(date_parser(row[int(index_col)]))

        dates = [(date[0], date[0] + relativedelta(months=date[1] - 1, day=31))
                 if date[1] != 0
                 else (date[0], date[0])
                 for date in dates]

        least_recent = dates[0][0]
        last = dates[0][1]
        for date in dates[1:]:
            if date[0] < least_recent:
                least_recent = date[0]
            if date[1] > last:
                last = date[1]
        interval = [least_recent, last]
        return interval
    return []


def find_columns_with_terms(csv_file):
    list_columns_with_date = []
    for index_row, row in enumerate(csv_file):
        columns_with_date = []
        for index_col, col in enumerate(row):
            if date_parser(col):
                columns_with_date.append(str(index_col))
        if columns_with_date:
            list_columns_with_date.append("|".join(columns_with_date))

    if list_columns_with_date:
        try:
            index = mode(list_columns_with_date)
            index = index.split("|")
        except StatisticsError:
            index = []

        return index
    return []



