from metadata_processor.config import config as conf_metadata_processor
from spatial_indexing.config import config as conf_spatial_indexing
from temporal_indexing.config import config as conf_temporal_indexing
from thematic_indexing.config import config as conf_thematic_indexing
from sqlalchemy import create_engine
from neo4j import GraphDatabase, basic_auth
import pysolr
from pythonping import ping


def run():
    ping('metadata_database', verbose=True)
    ping('temporal_index_database', verbose=True)
    ping('spatial_index_database', verbose=True)
    ping('thematic_index_solr', verbose=True)

    engine1 = create_engine(conf_metadata_processor.db_connection)

    with engine1.connect() as conn:
        test = conn.execute("""SELECT table_name
                               FROM information_schema.tables
                               WHERE table_schema='public'
                                 AND table_type='BASE TABLE';""").fetchall()
        if len(test) < 2:
            raise Exception("As tabelas não foram criadas ainda.")
        test = conn.execute("SELECT trigger_name FROM information_schema.triggers;").fetchall()
        if len(test) < 2:
            raise Exception("Os gatilhos não foram criadas ainda.")

    del engine1

    engine2 = create_engine(conf_temporal_indexing.db_connection)

    with engine2.connect() as conn:
        test = conn.execute("""SELECT table_name
                                   FROM information_schema.tables
                                   WHERE table_schema='public'
                                     AND table_type='BASE TABLE';""").fetchall()
        if len(test) < 1:
            raise Exception("As tabelas não foram criadas ainda.")
        test = conn.execute("""SELECT routine_name 
                               FROM information_schema.routines 
                               WHERE routine_type='FUNCTION' 
                                  AND specific_schema='public';""").fetchall()
        if len(test) < 4:
            raise Exception("Os procedimentos armazenados não foram criados ainda.")

    del engine2

    driver = GraphDatabase.driver(conf_spatial_indexing.db_connection["bolt_uri"],
                                  auth=basic_auth(conf_spatial_indexing.db_connection["user"],
                                                  conf_spatial_indexing.db_connection["password"]),
                                  encrypted=False)

    with driver.session() as session:
        test = session.run("match(p:Place) return count(p);").value()
        if test[0] < 5596:
            raise Exception("O grafo de lugares não foi totalmente criado.")

    del driver

    solr_resource = pysolr.Solr(conf_thematic_indexing.resource_solr_core_uri, always_commit=True)
    solr_resource.ping()
    solr_dataset = pysolr.Solr(conf_thematic_indexing.dataset_solr_core_uri, always_commit=True)
    solr_dataset.ping()

    del solr_dataset
    del solr_resource
