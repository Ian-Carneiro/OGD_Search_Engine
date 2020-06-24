import pysolr
from model import MetadataResources

solr = pysolr.Solr("http://localhost:8983/solr/main", always_commit=True)  # auth=<type of authentication>
solr.ping()


def run_thematic_indexing(resource: MetadataResources):
    metadata = f"{resource.name} {resource.description} {resource.package.title} {resource.package.notes} " \
               f"{resource.package.tags}"
    doc = {'id': resource.id, 'metadata': metadata, 'package_id': resource.package_id,
           'num_resources_package': resource.package.quant_resources}
    solr.add(doc)


# solr.add({'id': '123', 'metadata': {'add': 'foi comprar abacate'}, 'package_id': '321', 'num_resources_package': 4})

# 'package_id': '321',
          # 'package_metadata': {'add': 'zé das couves junior'}
# 'package_id': '321',
          # 'dataset_metadata': {'add': 'zé das couves junior'}

# {'id': '123', 'metadata': {'add': 'zé das couves junior'}, 'package_id': '321', 'num_resources_package': 4}