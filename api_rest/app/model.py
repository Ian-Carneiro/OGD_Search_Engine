from sqlalchemy import create_engine, text

engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/data_processor_db')


def get_resources(ids: list):
    sql = text("""select mr.description, mr."name", mr.id, mr.url, md.organization_name
                  from metadata_resources mr join metadata_dataset md on mr.package_id like md.id
                  where mr.id like any (:ids)
               """)
    result = engine.execute(sql, {"ids": ids}).fetchall()
    ids_dict = {}
    for i, id_ in enumerate(ids):
        ids_dict[id_] = i

    resources = []
    for r in result:
        resources.append({"description": r[0], "name": r[1], "id": r[2], "url": r[3], "organization_name": r[4],
                          "order": ids_dict[r[2]]})
    resources.sort(key=lambda elem: elem["order"])
    return resources


def get_dataset(ids: list):
    dataset_list = []
    resources_dict = {}
    ids_dict = {}
    for i, id_ in enumerate(ids):
        ids_dict[id_] = i
    sql = text("""select md.id, md.notes, md.title, md.organization_name, md.maintainer 
                  from metadata_dataset md
                  where md.id like any (:ids)
               """)
    dataset = engine.execute(sql, {"ids": ids}).fetchall()

    sql = text("""select mr."name", mr.description, mr.url, mr.format, mr.package_id 
                  from metadata_resources mr
                  where mr.package_id like any (:ids)
                   """)
    resources = engine.execute(sql, {"ids": ids}).fetchall()
    for r in resources:
        try:
            resources_dict[r[4]].append({"name": r[0], "description": r[1], "url": r[2], "format": r[3]})
        except KeyError:
            resources_dict[r[4]] = [{"name": r[0], "description": r[1], "url": r[2], "format": r[3]}]
    for ds in dataset:
        resource = resources_dict.get(ds[0])
        if not resource:
            resource = []
        dataset_list.append({"id": ds[0], "notes": ds[1], "title": ds[2], "organization_name": ds[3],
                             "maintainer": ds[4], "resources": resource, "order": ids_dict[ds[0]]})
    dataset_list.sort(key=lambda elem: elem["order"])
    return dataset_list
