from neo4j import GraphDatabase, basic_auth
from app.config import config

# precisa estar encrypted=False quando rodar local
driver = GraphDatabase.driver(config.spatial_config["db_connection"]["bolt_uri"],
                              auth=basic_auth(config.spatial_config["db_connection"]["user"],
                                              config.spatial_config["db_connection"]["password"]),
                              encrypted=False)


def get_place(query):
    with driver.session() as session:
        return session.run(f"""
            match (p1:Place)
            where toUpper(p1.nome)=toUpper("{query}")
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
            match (:Place {{gid:"{gid_place}"}})-[:`_CONTAINS`*0..]->(p:Place)<-[ht: HAS_TERM]-(r:Resource)
            with case p.gid when "{gid_place}" 
            then {{id: r.id, freq: ht.freq}} else {{id: r.id, freq: ht.freq*0.1}} end as rows
            return rows.id, sum(rows.freq)
        """).values()
        resources = result
    resources_dict = {}
    for r in resources:
        resources_dict[r[0]] = r[1]
    return resources_dict


def get_dataset(gid_place):
    with driver.session() as session:
        dataset_ids = session.run(f"""
        match (:Place {{gid:"{gid_place}"}})-[:`_CONTAINS`*0..]->(:Place)<-[:HAS_TERM]-(r:Resource)
        return r.package_id, count(distinct r.package_id)/r.num_package_resources as freq
        """).values()
    if not dataset_ids:
        return {}
    dataset_ids_dict = {}
    for id_ in dataset_ids:
        dataset_ids_dict[id_[0]] = id_[1]
    return dataset_ids_dict

# match (:Place {{gid:"{gid_place}"}})-[:`_CONTAINS`*0..]->(p:Place)<-[:HAS_TERM]-(r:Resource)
# with case p.gid when "{gid_place}"
# then {{id: r.id, package_id:r.package_id, num_package_resources:r.num_package_resources, mult:1}}
# else {{id: r.id, package_id:r.package_id, num_package_resources:r.num_package_resources, mult:0.1}} end as r
# with r.id as id, max(r.mult) as mult, r.package_id as package_id,
# r.num_package_resources as num_package_resources
# return package_id, (count(package_id)/num_package_resources)*max(mult) as freq
