import pysolr
from app.config import config

solr_resource = pysolr.Solr(config.thematic_config["resource_solr_core_uri"], always_commit=True)  # auth=<type of authentication>
solr_dataset = pysolr.Solr(config.thematic_config["dataset_solr_core_uri"], always_commit=True)  # auth=<type of authentication>
solr_resource.ping()
solr_dataset.ping()


def get_resource(query: str):
    docs = solr_resource.search(query, fl='id,score', rows=1000000000)
    ids = {}
    for doc in docs:
        doc['score'] /= 100
        ids[doc['id']] = doc['score']
    return ids


def get_dataset(query: str):
    docs = solr_dataset.search(query, fl='id,score', rows=1000000000)
    ids = {}
    for doc in docs:
        doc['score'] /= 100
        ids[doc['id']] = doc['score']
    return ids
