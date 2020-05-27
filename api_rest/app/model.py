from sqlalchemy import create_engine, text

engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/data_processor_db')


def get_resources(ids: list):
    sql = text("""select mr.description, mr."name", mr.id, mr.url, md.organization_name
                from (unnest((:ids)) as ids left join metadata_resources mr on mr.id like ids) as mr 
                left join metadata_dataset md on mr.package_id like md.id
               """)
    result = engine.execute(sql, {"ids": ids}).fetchall()
    resources = []
    for r in result:
        resources.append({"description": r[0], "name": r[1], "id": r[2], "url": r[3], "organization_name": r[4]})
    return resources


def get_dataset(ids: list):
    dataset_list = []
    resources_dict = {}
    sql = text("""select md.id, md.notes, md.title, md.organization_name, md.maintainer 
                  from unnest((:ids)) as ids left join metadata_dataset md on md.id like ids
               """)
    dataset = engine.execute(sql, {"ids": ids}).fetchall()

    sql = text("""select mr."name", mr.description, mr.url, mr.format, mr.package_id 
                  from unnest((:ids)) as ids join metadata_resources mr on mr.package_id like ids

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
                             "maintainer": ds[4], "resources": resource})
    return dataset_list
