from sqlalchemy.sql import text
from neo4j import GraphDatabase, basic_auth
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/data_processor_db')
driver = GraphDatabase.driver("bolt://0.0.0.0:7687", auth=basic_auth("neo4j", "neo4j"), encrypted=False)


def get_place(query):
    with driver.session() as session:
        return session.run(f"""
            match (p1:Place {{nome:"{query}"}})
            optional match (p1)<-[:`_CONTAINS`]-(p2:Place)
            return p1.gid, p1.nome, p1.tipo, p2.nome, p2.tipo
        """).values()


def find_places(name_place):
    places = get_place(name_place)
    place_dict = []
    for place in places:
        place_dict.append({'gid': place[0], 'name_more_specific': place[1], 'type_more_specific': place[2],
                           'name_less_specific': place[3], 'type_less_specific': place[4]})
    return place_dict


def get_resources(gid_place):
    with driver.session() as session:
        result = session.run(f"""
            match (p:Place {{gid:"{gid_place}"}})
            optional match(p)<-[ht: HAS_TERM]-(r:Resource)     
            with collect({{id:r.id, freq:ht.freq+1}}) as rows, p, collect(r.id) as ids
            optional match (p)-[:`_CONTAINS`*]->(:Place)<-[ht:HAS_TERM]-(r:Resource) 
            where not r.id in ids
            with rows+collect({{id:r.id, freq:ht.freq}}) as allRows
            UNWIND allRows as row
            return row.id, row.freq
        """).values()
        resources = result
    resources_dict = {}
    for r in resources:
        resources_dict[r[0]] = r[1]
    return resources_dict


def get_dataset(gid_place):
    with driver.session() as session:
        dataset_ids = session.run(f"""
        match (p:Place {{gid:"{gid_place}"}})
        optional match(p)<-[: HAS_TERM]-(r:Resource)     
        with collect({{id:r.id, package_id:r.package_id}}) as rows, p, count(distinct(r.package_id)) 
        as quant_first_packages
        optional match (p)-[:`_CONTAINS`*]->(:Place)<-[:HAS_TERM]-(r:Resource) 
        with rows+collect({{id:r.id, package_id:r.package_id}}) as allRows, quant_first_packages
        UNWIND allRows as row
        with distinct(row), quant_first_packages
        where row.package_id is not null
        return row.package_id, count(row.package_id), quant_first_packages
        """).values()
    if not dataset_ids:
        return {}

    first_dataset = []
    for i in range(dataset_ids[0][2]):
        first_dataset.append(dataset_ids[i][0])

    dataset_ids_aux = {}
    ids = []
    for db in dataset_ids:
        ids.append([db[0]])
        dataset_ids_aux[db[0]] = db[1]
    dataset_ids = dataset_ids_aux
    del dataset_ids_aux
    sql = text('''
        select md.id, md.quant_resources 
        from metadata_dataset md
        where md.id like any (:ids); 
    ''')
    total_quant_per_package = engine.execute(sql, {'ids': ids}).fetchall()
    del ids

    for d in total_quant_per_package:
        dataset_ids[d[0]] = (dataset_ids[d[0]] / d[1])

    for fd in first_dataset:
        dataset_ids[fd] = dataset_ids[fd]+1
    return dataset_ids

