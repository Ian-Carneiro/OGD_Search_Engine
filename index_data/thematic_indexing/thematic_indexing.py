import pysolr
from model import MetadataDataset
from index_log import log


solr_resource = pysolr.Solr("http://localhost:8983/solr/resource", always_commit=True)  # auth=<type of authentication>
solr_dataset = pysolr.Solr("http://localhost:8983/solr/dataset", always_commit=True)  # auth=<type of authentication>
solr_resource.ping()
solr_dataset.ping()


def run_thematic_dataset_indexing(dataset: MetadataDataset):
    indexed_resources_ids = []
    metadata_dataset = ""
    if len(dataset.resources) == 0:
        return []

    for metadata in [dataset.title, dataset.notes, dataset.tags]:
        metadata_dataset += f"{metadata} "

    metadata_resources = ""
    update = False
    if solr_dataset.search(f'id:{dataset.id}', fl='id').__len__() != 0:
        update = True

    for resource in dataset.resources:
        if resource.updated:
            log.info(f"recurso {resource.id} marcado para atualização")
            update = True
            solr_resource.delete(id=resource.id)

    if update:
        solr_dataset.delete(id=dataset.id)
        docs = solr_resource.search(f"package_id:{dataset.id}", fl='metadata', rows=1000000000)
        for doc in docs:
            metadata_resources += f"{doc['metadata'].replace(metadata_dataset, '')} "

    for resource in dataset.resources:
        log.info('id_resource: ' + resource.id)
        metadata_resource = ""
        for metadata in [resource.name, resource.description]:
            metadata_resource += f"{metadata} "
        solr_resource.add({'id': resource.id, 'package_id': resource.package_id,
                           'metadata': metadata_resource + metadata_dataset})
        indexed_resources_ids.append(resource.id)
        metadata_resources += f"{metadata_resource} "
    metadata_dataset += f" {metadata_resources}"
    solr_dataset.add({'id': dataset.id, 'metadata': metadata_dataset})
    return indexed_resources_ids


def delete_thematic_indexes(dataset: MetadataDataset):
    docs = solr_dataset.search(f"id:{dataset.id}", fl='metadata')
    if not docs:
        return
    for doc in docs:
        metadata_dataset = doc['metadata']
    for resource in dataset.resources:
        solr_resource.delete(id=resource.id)
        metadata_resource = ''
        for metadata in [resource.name, resource.description]:
            metadata_resource += f"{metadata} "
        metadata_dataset = metadata_dataset.replace(metadata_resource, '')
    solr_dataset.delete(id=dataset.id)
    solr_dataset.add({'id': dataset.id, 'metadata': metadata_dataset})
