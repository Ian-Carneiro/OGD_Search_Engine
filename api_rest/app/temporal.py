# from dateutil.relativedelta import relativedelta
from datetime import date
from sqlalchemy import create_engine, text

engine_temporal_indexing_module = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5431'
                                                '/temporal_indexing_module')

# todos os métodos comentados já foram passados para procedimentos armazenados no banco de dados
#
# def distance_between_dates(interval_start: date, interval_end: date):
#     interval_start_month = interval_start.month
#     interval_end_month = interval_end.month
#     interval_start_year = interval_start.year
#     interval_end_year = interval_end.year
#     interval_start_day = interval_start.day
#     interval_end_day = interval_end.day
#
#     if interval_end_month == interval_start_month and interval_start_year == interval_end_year:
#         last_day_of_month = interval_start + relativedelta(day=31)
#         months = (interval_end_day - (interval_start_day-1)) / last_day_of_month.day
#     else:
#         last_day_of_month = (interval_start + relativedelta(day=31)).day
#         first_month = (last_day_of_month - (interval_start_day - 1)) / last_day_of_month
#         last_day_of_month = (interval_end + relativedelta(day=31)).day
#         last_month = interval_end_day / last_day_of_month
#         months = first_month + last_month + ((interval_end_month - interval_start_month) - 1) + \
#                  (12 * (interval_end_year - interval_start_year))
#
#     return months
#
#
# def get_interval_intersection(interval_start_1: date, interval_end_1: date, interval_start_2: date,
#                               interval_end_2: date):
#     if interval_end_1 < interval_start_2 or interval_start_1 > interval_end_2:
#         return 0
#     if interval_start_1 <= interval_start_2:
#         aux_interval_start = interval_start_2
#     else:
#         aux_interval_start = interval_start_1
#
#     if interval_end_1 >= interval_end_2:
#         aux_interval_end = interval_end_2
#     else:
#         aux_interval_end = interval_end_1
#
#     months = distance_between_dates(aux_interval_start, aux_interval_end)
#     return months
#
#
# def get_interval_difference(interval_start_1: date, interval_end_1: date, interval_start_2: date,
#                             interval_end_2: date):
#     one_day = relativedelta(days=1)
#     if interval_start_1 >= interval_start_2 and interval_end_1 <= interval_end_2:
#         return 0
#     elif interval_end_2 < interval_start_1 or interval_start_2 > interval_end_1:
#         months = distance_between_dates(interval_start_1, interval_end_1)
#     elif interval_start_1 < interval_start_2 and interval_end_1 > interval_end_2:
#         months = distance_between_dates(interval_start_1, interval_start_2 - one_day)
#         months += distance_between_dates(interval_end_2 + one_day, interval_end_1)
#     elif interval_start_1 >= interval_start_2:
#         months = distance_between_dates(interval_end_2 + one_day, interval_end_1)
#     elif interval_end_1 <= interval_end_2:
#         months = distance_between_dates(interval_start_1, interval_start_2 - one_day)
#     return months
#
#
# def get_similarity(interval_start_1: date, interval_end_1: date, interval_start_2: date, interval_end_2: date):
#     alpha = 0.5
#     beta = 0.5
#     intersection = get_interval_intersection(interval_start_1, interval_end_1, interval_start_2, interval_end_2)
#     if intersection == 0:
#         return 0
#     difference_ab = get_interval_difference(interval_start_1, interval_end_1, interval_start_2, interval_end_2)
#     difference_ba = get_interval_difference(interval_start_2, interval_end_2, interval_start_1, interval_end_1)
#     similarity = intersection/(intersection + alpha * difference_ab + beta * difference_ba)
#     return similarity


def get_resource(interval_start: date, interval_end: date):
    sql = text("""
    select * from
    (select ti.id_resource, get_similarity(:interval_start_, :interval_end_, ti.interval_start, ti.interval_end, 0.5, 0.5) 
    as similarity
    from temporal_index ti) as tab
    where similarity > 0.0 
    """)

    index = engine_temporal_indexing_module\
        .execute(sql, {'interval_start_': interval_start, 'interval_end_': interval_end}).fetchall()
    resources = {}
    for i in index:
        resources[i[0]] = i[1]
    return resources


def get_dataset(interval_start: date, interval_end: date):
    sql = text("""select ti.package_id, count(ti.package_id)/ti.num_resources_package :: float
                  from temporal_index ti
                  where get_similarity(:interval_start_, :interval_end_, ti.interval_start, ti.interval_end, 0.5, 0.5)>0
                  group by ti.package_id, ti.num_resources_package""")
    index = engine_temporal_indexing_module\
        .execute(sql, {'interval_start_': interval_start, 'interval_end_': interval_end}).fetchall()
    resources = {}
    for i in index:
        resources[i[0]] = i[1]
    return resources
