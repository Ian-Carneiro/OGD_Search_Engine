from sqlalchemy import create_engine, text
from temporal_indexing.config import config

engine = create_engine(config.db_connection)


def delete_temporal_index(resource_id):
    with engine.connect() as conn:
        sql = text("delete from temporal_index where id_resource like :id_resource")
        conn.execute(sql, id_resource=resource_id)


def insert_index(resource_id, interval_start, interval_end, package_id, num_package_resources):
    with engine.connect() as conn:
        sql = text("insert into temporal_index"
                   "(id_resource, interval_start, interval_end, package_id, num_resources_package) "
                   "values(:id_resource, :interval_start, :interval_end, :package_id, :num_resources_package)")
        conn.execute(sql, id_resource=resource_id, interval_start=interval_start, interval_end=interval_end,
                     package_id=package_id, num_resources_package=num_package_resources)


def update_num_resources(package_id, num_package_resources):
    with engine.connect() as conn:
        sql = text("update temporal_index set num_resources_package = :num_resources_package "
                   "where package_id like :package_id")
        conn.execute(sql, num_resources_package=num_package_resources, package_id=package_id)
