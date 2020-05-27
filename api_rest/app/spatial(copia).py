from sqlalchemy.sql import text
from neo4j import GraphDatabase, basic_auth
import time
from flask import jsonify, request, make_response
from sqlalchemy import create_engine

from app import app

engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/data_processor_db')
# driver = GraphDatabase.driver("bolt://0.0.0.0:7687", auth=basic_auth("neo4j", "neo4j"), encrypted=False)


def get_place(query):
    with driver.session() as session:
        return session.run(f"""
            match (p1:Place {{nome:"{query}"}})
            optional match (p1)<-[:`_CONTAINS`]-(p2:Place)
            return p1.gid, p1.nome, p1.tipo, p2.nome, p2.tipo
        """).values()


@app.route('/api/v1/ogdse/place/search/resource')
def find_places_or_resources():
    limit = 30
    page = int(request.args.get('page'))
    gid_place = request.args.get('gid_place')
    name_place = request.args.get('name_place')

    if gid_place and not name_place:
        response = get_resources(gid_place, limit, page)
    elif name_place and not gid_place:
        tic = time.time()
        places = get_place(name_place)
        tac = time.time()
        print(tac - tic, 's verificar lugar específico')
        if len(places) > 1:
            place_dict = []
            for place in places:
                place_dict.append({'gid': place[0], 'name_more_specific': place[1], 'type_more_specific': place[2],
                                   'name_less_specific': place[3], 'type_less_specific': place[4]})
            response = make_response(jsonify(place_dict), 300)
        elif len(places) == 0:
            response = jsonify([])
        else:
            response = get_resources(places[0][0], limit, page)
    else:
        response = jsonify([])
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


def get_resources(gid_place, limit, page):
    resources = []
    time_i = time.time()
    with driver.session() as session:
        result = session.run(f"""
            match (p:Place {{gid:"{gid_place}"}})<-[ht:HAS_TERM]-(r:Resource) 
            return r.id, r.name, r.description, r.package_id, r.organization_name, r.url, ht.freq
            order by ht.freq desc skip {int(page - 1) * limit} limit {limit}
        """).values()
        resources += result
        len_resources = len(resources)
        # 32 é o valor da última UF do banco :)
        if len_resources < 30 and int(gid_place) <= 32:
            if len_resources > 0:
                result = session.run(f"""
                    match (p:Place {{gid:"{gid_place}"}})-[:_CONTAINS*]->(p1:Place)<-[ht:HAS_TERM]-(r:Resource) 
                    return distinct r.id, r.name, r.description, r.package_id, r.organization_name, r.url, ht.freq
                    order by ht.freq desc limit {limit - len_resources}
                """).values()
                resources += result
            else:
                total = session.run(f"""
                    match (p:Place {{gid:"{gid_place}"}})<-[:HAS_TERM]-(r:Resource) 
                    return count(r)""").value()
                complete_pages, remainder = divmod(total[0], limit)
                skip = ((int(page) - 1 - complete_pages) * limit) - remainder
                resources = session.run(f"""
                    match (p:Place {{gid:"{gid_place}"}})-[:_CONTAINS*]->(p1:Place)<-[ht:HAS_TERM]-(r:Resource) 
                    return distinct(r.id), r.name, r.description, r.package_id, r.organization_name, 
                    r.url, sum(ht.freq) as freq
                    order by freq desc 
                    skip {skip} limit {limit}
                """).values()
                resources += result

    time_f = time.time()
    print(time_f - time_i, 's buscar recursos')

    resources_dict = []
    for r in resources:
        resources_dict.append({'url': r[5], 'description': r[2], 'name': r[1], 'organization': r[4],
                               'package_id': r[3], 'freq': r[6]})
    response = jsonify(resources_dict)
    return response


@app.route('/api/v1/ogdse/place/search/dataset')
def find_places_or_dataset():
    limit = 30
    page = int(request.args.get('page'))
    gid_place = request.args.get('gid_place')
    name_place = request.args.get('name_place')

    if gid_place and not name_place:
        response = get_database(gid_place, limit, page)
    elif name_place and not gid_place:
        tic = time.time()
        places = get_place(name_place)
        tac = time.time()
        print(tac - tic, 's verificar lugar específico')
        if len(places) > 1:
            place_dict = []
            for place in places:
                place_dict.append({'gid': place[0], 'name_more_specific': place[1], 'type_more_specific': place[2],
                                   'name_less_specific': place[3], 'type_less_specific': place[4]})
            response = make_response(jsonify(place_dict), 300)
        elif len(places) == 0:
            response = jsonify([])
        else:
            response = get_database(places[0][0], limit, page)
    else:
        response = jsonify([])
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


def get_database(gid_place, limit, page):
    dataset_ids = []
    with driver.session() as session:
        result = session.run(f"""
            match(:Place{{gid: "{gid_place}"}})<-[: HAS_TERM]-(r:Resource)                
            return r.package_id, count(r.package_id) as quant_packages
            skip {limit}*({page}-1) limit {limit}
        """).values()
        dataset_ids += result
        len_dataset_ids = len(dataset_ids)

        if len_dataset_ids < 30 and int(gid_place) <= 32:
            if len_dataset_ids > 0:
                result = session.run(f"""
                            match (:Place {{gid:"{gid_place}"}})-[:`_CONTAINS`*]->(p1:Place)<-[:HAS_TERM]-(r:Resource) 
                            return r.package_id, count(r.package_id) as quant_packages 
                            limit {limit - len_dataset_ids}
                            """).values()
                dataset_ids += result
            else:
                total = session.run(f"""
                    match (p:Place {{gid:"{gid_place}"}})<-[:HAS_TERM]-(r:Resource) 
                    return count(distinct(r.package_id))""").value()
                complete_pages, remainder = divmod(total[0], limit)
                skip = ((int(page) - 1 - complete_pages) * limit) - remainder
                result = session.run(f"""
                    match (p:Place {{gid:"{gid_place}"}})-[:_CONTAINS*]->(p1:Place)<-[ht:HAS_TERM]-(r:Resource)
                    return r.package_id, count(r.package_id) as quant_packages 
                    skip {skip} limit {limit}
                """).values()
                dataset_ids += result

    dataset_list = []
    for id_ in dataset_ids:
        sql = text('''select md.notes, md.title, md.organization_name, md.maintainer, 
                                :quant/tab.total_packages::float as freq
                        from (
                            select package_id , count(mr.package_id) total_packages
                            from metadata_resources mr
                            where package_id like :id
                            group by mr.package_id ) tab, metadata_dataset md
                        where tab.package_id ilike md.id ''')
        res1 = engine.execute(sql, {'quant': id_[1], 'id': id_[0]}).fetchone()
        sql = text('''
                select mr."name", mr.description, mr.url, mr.format 
                from metadata_resources mr 
                where mr.package_id like :id
        ''')
        res2 = engine.execute(sql, {'id': id_[0]}).fetchall()

        dataset_list.append({'id': id_[0], 'notes': res1[0], 'title': res1[1], 'organization_name': res1[2],
                             'maintainer': res1[3], 'frequency': res1[4],
                             'resources': [{'name': r[0], 'description': r[1], 'url': r[2], 'format': r[3]}
                                           for r in res2]})

    def take_frequency(elem):
        return elem['frequency']

    dataset_list.sort(key=take_frequency, reverse=True)
    response = jsonify(dataset_list)
    return response
