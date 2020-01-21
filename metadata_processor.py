from ckanapi import RemoteCKAN
import json
import pandas
from sqlalchemy import create_engine
import time
import schedule
from threading import Thread

# portaisUrls = ['http://dados.gov.br', 'http://dados.al.gov.br', 'http://dados.recife.pe.gov.br', 'https://dados.rs.gov.br', 'http://datapoa.com.br', 'http://dados.fortaleza.ce.gov.br', 'http://dados.prefeitura.sp.gov.br']
# dados_gov = RemoteCKAN('http://dados.al.gov.br')
# dados_gov = RemoteCKAN('http://dados.recife.pe.gov.br')
# dados_gov = RemoteCKAN('https://dados.rs.gov.br')
# dados_gov = RemoteCKAN('http://datapoa.com.br')
# dados_gov = RemoteCKAN('http://www.data.rio')# Não funciona
# dados_gov = RemoteCKAN('http://dados.fortaleza.ce.gov.br')
# dados_gov = RemoteCKAN('http://dados.prefeitura.sp.gov.br')

dados_gov = RemoteCKAN('http://dados.gov.br')
assert dados_gov.action.site_read()
engine = create_engine('postgresql+psycopg2://postgres:postgres@db_postgres_tcc:5432/data_processor_db')


def download_and_persist():
    engine.execute("DROP TABLE IF EXISTS metadata_resources;")
    engine.execute("DROP TABLE IF EXISTS metadata_dataset;")
    page = 0
    total_time_persistence = 0
    total_time_process_data = 0
    total_time_download = 0

    while True:
        print("Page: ", page)
        ti = time.time()
        metadata = dados_gov.action.current_package_list_with_resources(limit=1000, offset=1000 * page)
        total_time_download += time.time()-ti

        ti = time.time()
        page += 1

        new_metadata = pandas.io.json.json_normalize(metadata)
        new_metadata = new_metadata[['maintainer', 'id', 'author', 'state', 'name', 'url', 'notes', 'metadata_created',
                                     'metadata_modified', 'title', 'license_title']]

        resources = pandas.io.json.json_normalize(metadata, 'resources')
        resources = resources[['id', 'package_id', 'url', 'description', 'name', 'format', 'created',
                               'last_modified', 'state']]

        metadata = pandas.read_json(json.dumps(metadata))
        tags = []
        organizations = []
        organization_id = []
        for index, m in metadata.iterrows():
            try:
                organizations.append(m['organization']['name'])
                organization_id.append(m['organization']['id'])
            except TypeError:
                organizations.append(None)
                organization_id.append(None)
            tags.append(", ".join([tag['name'] for tag in m['tags']]))

        new_metadata['tags'] = tags
        new_metadata['organization_name'] = organizations
        new_metadata['organization_id'] = organization_id
        total_time_process_data += time.time() - ti

        ti = time.time()
        new_metadata.to_sql(name='metadata_dataset', con=engine, if_exists='append', index=False)
        resources.to_sql(name='metadata_resources', con=engine, if_exists='append', index=False)
        total_time_persistence += time.time() - ti
        if len(metadata) < 1000:
            break

    print("totalTimePersistence:", total_time_persistence)
    print("totalTimeDownload:", total_time_download)
    print("totalTimeProcessData:", total_time_process_data)


def agendador():
    schedule.every().day.at("00:00").do(download_and_persist)
    while True:
        schedule.run_pending()
        time.sleep(1)


def run():
    download_and_persist()
    thread = Thread(target=agendador)
    thread.start()
    # agendador()
