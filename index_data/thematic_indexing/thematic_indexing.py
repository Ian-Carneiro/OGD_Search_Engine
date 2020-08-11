import pysolr
from model import MetadataResources, MetadataDataset
from sys import getsizeof

solr_resource = pysolr.Solr("http://localhost:8983/solr/resource", always_commit=True)  # auth=<type of authentication>
solr_dataset = pysolr.Solr("http://localhost:8983/solr/dataset", always_commit=True)  # auth=<type of authentication>
solr_resource.ping()
solr_dataset.ping()


def run_thematic_dataset_indexing(dataset: MetadataDataset):
    metadata_dataset = ""
    if len(dataset.resources) == 0:
        return

    for metadata in [dataset.title, dataset.notes, dataset.tags]:
        metadata_dataset += f"{metadata}\n"
    metadata_resources = ""
    for resource in dataset.resources:
        metadata_resource = ""
        for metadata in [resource.name, resource.description, metadata_dataset]:
            metadata_resource += f"{metadata}\n"
        # print("resource metadata size(kb): ", getsizeof(metadata_resource)/1024)
        solr_resource.add({'id': resource.id, 'metadata': metadata_resource})
        metadata_resources += f"{metadata_resource}\n"
    metadata_dataset += f"\n{metadata_resources}"
    # print("dataset metadata size(kb): ", getsizeof(metadata_dataset)/1024)
    solr_dataset.add({'id': dataset.id, 'metadata': metadata_dataset})
