from datetime import date
from sqlalchemy import create_engine, text
from app.config import config

engine_temporal_indexing_module = create_engine(config.temporal_config['db_connection'])


def get_resource(interval_start: date, interval_end: date):
    sql = text("""
    select * from
    (select ti.id_resource, get_similarity(:interval_start_, :interval_end_, ti.interval_start, ti.interval_end, 
    :alpha, :beta) 
    as similarity
    from temporal_index ti) as tab
    where similarity > 0.0 
    """)

    index = engine_temporal_indexing_module\
        .execute(sql, {'interval_start_': interval_start,
                       'interval_end_': interval_end,
                       'alpha': config.temporal_config['alpha_value'],
                       'beta': config.temporal_config['beta_value']}).fetchall()
    resources = {}
    for i in index:
        resources[i[0]] = i[1]
    return resources


def get_dataset(interval_start: date, interval_end: date):
    sql = text("""select ti.package_id, count(ti.package_id)/ti.num_resources_package :: float
                  from temporal_index ti
                  where get_similarity(:interval_start_, :interval_end_, ti.interval_start, ti.interval_end, 
                  :alpha, :beta)>0
                  group by ti.package_id, ti.num_resources_package""")
    index = engine_temporal_indexing_module\
        .execute(sql, {'interval_start_': interval_start,
                       'interval_end_': interval_end,
                       'alpha': config.temporal_config['alpha_value'],
                       'beta': config.temporal_config['beta_value']}).fetchall()
    resources = {}
    for i in index:
        resources[i[0]] = i[1]
    return resources
