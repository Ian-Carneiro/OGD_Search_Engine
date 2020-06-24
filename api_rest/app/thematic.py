import pysolr

solr = pysolr.Solr("http://localhost:8983/solr/main", always_commit=True)  # auth=<type of authentication>
solr.ping()


def get_resource(query: str):
    docs = solr.search(query, fl='id,score', rows=1000000000)
    ids = {}
    for doc in docs:
        doc['score'] /= 100
        ids[doc['id']] = doc['score']
    return ids


def get_dataset(query: str):
    docs = solr.search(query, **{'fl': 'num_resources_package', 'omitHeader': 'true', 'group': 'true',
                       'group.field': 'package_id', 'group.limit': 1, 'rows': 1000000000})
    ids = {}
    for doc in docs.grouped['package_id']['groups']:
        doc_list = doc['doclist']
        ids[doc['groupValue']] = doc_list['numFound']/doc_list['docs'][0]['num_resources_package']
    return ids
