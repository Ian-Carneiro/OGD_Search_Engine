def return_type_place(place, driver):
    with driver.session() as session:
        result = session.run("""match (p:Place)
                                where toUpper(p.nome) = toUpper("{}")
                                return distinct p.tipo""".format(place)).value()
        if len(result) > 1:
            return "UNDEFINED"
        elif not result:
            return None
        else:
            return result[0]


def insert_into_resource_place(gid, id_resource, driver):
    with driver.session() as session:
        resource = session.run("""
            match (p:Place {{gid:"{}"}})
            merge (p)<-[:HAS_TERM]-(r: Resource{{id: "{}"}})
            on create set r.quant = 1
            on match set r.quant = r.quant+1
        """.format(gid, id_resource))


def find_places_and_index(id_resource, places_in_order: [], types_in_order: [], driver):
    len_places_in_order = len(places_in_order)
    specific_places = set()
    nonspecific_places = set()
    index_compared_places = set()
    # Caso haja somente locais do mesmo tipo
    if len(set(types_in_order)) == 1:
        with driver.session() as session:
            for i in range(len_places_in_order):
                result = session.run("""
                    match(p:Place)
                    where toUpper(p.nome) = toUpper("{}") and toUpper(p.tipo) = toUpper("{}")
                    return p.gid
                """.format(places_in_order[i], types_in_order[i])).value()
                specific_places.update(result)
    else:
        with driver.session() as session:
            for i in range(len_places_in_order):
                type_place_i = types_in_order[i]
                for j in range(len_places_in_order):
                    type_place_j = types_in_order[j]
                    if not (type_place_j == "MUNICÍPIO" and type_place_i in ["REGIÃO", "UF"] or
                            type_place_j == "UF" and type_place_i == "REGIÃO" or
                            type_place_j == type_place_i):
                        result = session.run("""
                            match (p1:Place)<-[c:_CONTAINS*]-(p2:Place)
                            where toUpper(p1.nome) = toUpper("{0}") and 
                                  toUpper(p1.tipo) = toUpper("{2}") and 
                                  toUpper(p2.nome) = toUpper("{1}") and 
                                  toUpper(p2.tipo) = toUpper("{3}")
                            return p1.gid, p2.gid
                        """.format(places_in_order[i], places_in_order[j], type_place_i, type_place_j)).values()
                        if result:
                            index_compared_places.add(i)
                            index_compared_places.add(j)
                            nonspecific_places.add(result[0][1])
                            specific_places.add(result[0][0])
            specific_places = specific_places - nonspecific_places
            # Caso haja locais que não estão contidos ou que não contêm
            if len(index_compared_places) < len_places_in_order:
                for i in range(len_places_in_order):
                    if i not in index_compared_places:
                        result = session.run("""
                            match (p:Place)
                            where toUpper(p.nome) = toUpper("{}") and toUpper(p.tipo) = toUpper("{}")
                            return p.gid
                        """.format(places_in_order[i], types_in_order[i])).value()
                        specific_places.update(result)
    for p in specific_places:
        insert_into_resource_place(p, id_resource, driver)
    if len(specific_places) > 0:
        add_frequency_attribute(id_resource, driver)
        return True
    return False


def add_frequency_attribute(id_resource, driver):
    with driver.session() as session:
        total = session.run("""
            match (r:Resource {{id:"{0}"}})
            return sum(r.quant)
        """.format(id_resource)).value()[0]

        result = session.run("""
            merge (r:Resource {{id:"{0}"}})
            on match set r.freq = r.quant*100/toFloat({1}), r.total = {1}
            return r
        """.format(id_resource, total))


# from neo4j import GraphDatabase, basic_auth
#
# driver_ = GraphDatabase.driver("bolt://0.0.0.0:7687", auth=basic_auth("neo4j", "neo4j"), encrypted=False)
#
# with driver_.session() as session:
#     ids = session.run("""
#         match (r:Resource) return distinct r.id
#     """).value()
#
#     for i in ids:
#         print(i)
#         add_frequency_attribute(i, driver_)
