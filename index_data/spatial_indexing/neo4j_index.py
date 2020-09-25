from neo4j import GraphDatabase, basic_auth
from spatial_indexing.config import config

sort_ordering_criteria = {'MUNICÍPIO': 3, 'UF': 2, 'REGIÃO': 1}


def return_type_place(place, driver):
    with driver.session() as session:
        result = session.run("""match (p:Place)
                            where toUpper(p.{1}) = toUpper("{0}")
                            return distinct p.tipo""".format(place, 'nome' if len(place) != 2 else 'sigla')).value()

        if len(result) > 1:
            return "UNDEFINED"
        elif not result:
            return None
        else:
            return result[0]


def insert_into_resource_place(gid, resource, num_package_resources, total, quant, driver):
    with driver.session() as session:
        session.run("""
            match (p:Place {{gid:"{0}"}})
            merge (r: Resource{{id: "{1}", package_id: "{2}", total:{3}, num_package_resources: {4}}})
            create (p)<-[ht:HAS_TERM{{freq:{5}/toFloat(r.total), quant:{5}}}]-(r)
        """.format(gid, resource.id, resource.package_id, total, num_package_resources, quant))


def update_num_resources(package_id, num_package_resources, driver):
    with driver.session() as session:
        session.run(f"""
            match(r:Resource{{package_id:"{package_id}"}})
            set r.num_package_resources = {num_package_resources}
        """)


def delete_spatial_index(resource_id, driver):
    with driver.session() as session:
        session.run(f"""
            match(r: Resource {{id: "{resource_id}"}}) detach delete r;
        """)


class Neo4jIndex:
    def __init__(self):
        self._driver = GraphDatabase.driver(config.db_connection["bolt_uri"],
                                            auth=basic_auth(config.db_connection["user"],
                                                            config.db_connection["password"]),
                                            encrypted=False)

    def close(self):
        self._driver.close()

    def find_places_return_id(self, places_in_order: [], types_in_order: []):
        len_places_in_order = len(places_in_order)
        specific_places = set()
        with self._driver.session() as session:
            if len(set(types_in_order)) == 1:
                for i in range(len_places_in_order):
                    query = """
                        match(p:Place)
                        where toUpper(p.{}) = toUpper("{}") and toUpper(p.tipo) = toUpper("{}")
                        return p.gid
                    """.format('sigla' if len(places_in_order[i]) == 2 else 'nome',
                               places_in_order[i], types_in_order[0])
                    result = session.run(query).value()
                    specific_places.update(result)
            else:
                nonspecific_places = set()
                verified_places = set()
                types_and_places_in_order = [[types_in_order[i], places_in_order[i]] for i in
                                             range(len_places_in_order)]
                types_and_places_in_order.sort(key=lambda val: sort_ordering_criteria[val[0]])
                for i in range(len_places_in_order-1, 0, -1):
                    place_i = types_and_places_in_order[i][1]
                    for j in range(i):
                        place_j = types_and_places_in_order[j][1]
                        if place_i != place_j:
                            query = """
                                match (p1:Place)-[c:_CONTAINS*]->(p2:Place)
                                where toUpper(p1.{0}) = toUpper("{2}") and 
                                      toUpper(p1.tipo) = toUpper("{4}") and 
                                      toUpper(p2.{1}) = toUpper("{3}") and 
                                      toUpper(p2.tipo) = toUpper("{5}")
                                return p1.gid, p2.gid
                            """.format(
                                'sigla' if len(place_j) == 2 else 'nome',
                                'sigla' if len(place_i) == 2 else 'nome',
                                place_j, place_i,
                                types_and_places_in_order[j][0], types_and_places_in_order[i][0]
                            )
                            result = session.run(query).values()
                            if result:
                                for value in result:
                                    specific_places.add(value[1])
                                    nonspecific_places.add(value[0])
                                    verified_places.update([i, j])
                if len(verified_places) != len_places_in_order:
                    for i in range(len_places_in_order):
                        if not verified_places.__contains__(i):
                            query = """
                                    match(p:Place)
                                    where toUpper(p.{}) = toUpper("{}") and toUpper(p.tipo) = toUpper("{}")
                                    return p.gid
                                """.format('sigla' if len(places_in_order[i]) == 2 else 'nome',
                                           types_and_places_in_order[i][1], types_and_places_in_order[i][0])
                            result = session.run(query).value()
                            specific_places.update(result)
                specific_places.difference(nonspecific_places)
            return specific_places
