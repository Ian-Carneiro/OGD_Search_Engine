from ckanapi import RemoteCKAN
import pandas as pd
from sqlalchemy import create_engine
from index_log import log
from time import time
from metadata_processor.config import config

engine = create_engine(config.db_connection)


def download_and_persist_metadata():
    log.info('#----------------------------------------------------------------------------------------------#')
    log.info("Baixando e persistindo metadados.")
    log.info(f'Estabelecento conexão com {config.data_portal}...')
    dados_gov = RemoteCKAN(config.data_portal)
    assert dados_gov.action.site_read()
    log.info('conexão estabelecida.')
    with engine.connect() as conn:
        conn.execute("UPDATE metadata_resources SET excluded=TRUE;")
    page = 0
    time0 = time()
    limit = config.metadata['limit']
    offset = config.metadata['offset']
    while True:
        log.info(f"Página(até {limit} metadados de recursos): " + str(page))
        metadata = dados_gov.action.current_package_list_with_resources(limit=limit,
                                                                        offset=offset * page)

        page += 1

        new_metadata = pd.json_normalize(metadata)
        dataset = new_metadata[['id', 'maintainer', 'author', 'name', 'url', 'notes', 'metadata_created', 'tags',
                                'metadata_modified', 'title']]
        resources = pd.json_normalize(metadata, 'resources')
        resources = resources[['id', 'package_id', 'url', 'description', 'name', 'format', 'created', 'last_modified']]

        num_csv = resources['format'].eq('CSV').astype(int).groupby(resources['package_id']).sum()
        dataset = pd.merge(dataset, num_csv, left_on='id', right_on='package_id', how='left')\
            .rename(columns={'format': 'num_resources'})
        resources['spatial_indexing'] = False
        resources['temporal_indexing'] = False
        resources['thematic_indexing'] = False
        resources['updated'] = False
        resources['excluded'] = False

        tags = []
        organizations = []
        organization_id = []
        for m in metadata:
            try:
                organizations.append(m['organization']['name'])
                organization_id.append(m['organization']['id'])
            except TypeError:
                organizations.append(None)
                organization_id.append(None)
            tags.append(", ".join([tag['name'] for tag in m['tags']]))
        dataset['tags'] = tags
        dataset['organization_name'] = organizations
        dataset['organization_id'] = organization_id
        dataset['temporal_indexing'] = False
        dataset['thematic_indexing'] = False

        dataset.to_sql(name='metadata_dataset', con=engine, if_exists='append', index=False)
        resources.to_sql(name='metadata_resources', con=engine, if_exists='append', index=False)

        if len(metadata) < limit:
            break
    time1 = time()
    log.info("Tempo para baixar e persistir os metadados: " + str(time1-time0) + 's')
    with engine.connect() as conn:
        num_dataset = conn.execute("SELECT count(*) FROM metadata_dataset;").fetchone()
        num_resources = conn.execute("SELECT count(*) FROM metadata_resources;").fetchone()
        log.info("Quantidade de conjuntos de dados: " + str(num_dataset))
        log.info("Quantidade de recursos: " + str(num_resources))
    dados_gov.close()
