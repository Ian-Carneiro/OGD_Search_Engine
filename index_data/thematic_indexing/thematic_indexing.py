import pysolr
from model import MetadataDataset
from index_log import log
from thematic_indexing.config import config


solr_resource = pysolr.Solr(config.resource_solr_core_uri, always_commit=True)  # auth=<type of authentication>
solr_dataset = pysolr.Solr(config.dataset_solr_core_uri, always_commit=True)  # auth=<type of authentication>
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
    if solr_dataset.search(f'id:{dataset.id}', fl='id').__len__() != 0:
        log.info(f"Atualizando recursos e conjunto de dados")
        solr_resource.delete(f"package_id:{dataset.id}")
        solr_dataset.delete(f"id:{dataset.id}")

    for resource in dataset.resources:
        log.info('id_resource: ' + resource.id)
        metadata_resource = ""
        for metadata in [resource.name, resource.description]:
            metadata_resource += f"{metadata} "
        solr_resource.add({'id': resource.id, 'package_id': resource.package_id,
                           'metadata': metadata_resource + metadata_dataset})
        if not resource.thematic_indexing:
            indexed_resources_ids.append(resource.id)
        metadata_resources += f"{metadata_resource} "
    metadata_dataset += f" {metadata_resources}"
    solr_dataset.add({'id': dataset.id, 'metadata': metadata_dataset})
    return indexed_resources_ids


def delete_thematic_indexes(resource):
    log.info(f'removendo índice temático de resource {resource.id}')
    solr_resource.delete(id=resource.id)
    if len(solr_resource.search(f'package_id:{resource.package_id}', rows=1)) == 0:
        solr_dataset.delete(id=resource.package_id)
        return
    metadata_resource = ''
    for metadata in [resource.name, resource.description]:
        metadata_resource += f"{metadata} "
    doc = solr_dataset.search(f'id:{resource.package_id}').docs
    doc = doc[0]
    doc['metadata'] = doc['metadata'].replace(metadata_resource, '')
    solr_dataset.delete(id=resource.package_id)
    solr_dataset.add({'id': doc['id'], 'metadata': doc['metadata']})
