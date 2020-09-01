from model import MetadataDataset
from .date_finder import date_parser
from datetime import datetime
from dateutil.relativedelta import relativedelta
from index_log import log
from temporal_indexing.postgres_index import delete_temporal_index, insert_index, update_num_resources


def run_temporal_index(resource, num_resources_package, package_title, package_notes):
    funcs = [lambda: (date_parser(resource.name), f"resource.name: {resource.name}"),
             lambda: (date_parser(resource.description), f"resource.description: {resource.description}"),
             lambda: (date_parser(package_title), f"package.title: {package_title}"),
             lambda: (date_parser(package_notes), f"package.notes: {package_notes}"),
             ]
    for func in funcs:
        dates = func()
        if dates[0]:
            break
    # -----------------------------------------------------------------------------------------------------------------
    if dates[0]:
        log.info(f"Datas em {dates[1]}")

    if len(dates[0]) == 0:
        interval = None
        if resource.created:
            date = datetime.strptime(resource.created, '%Y-%m-%dT%H:%M:%S.%f')
            interval = (date, date)
            if interval:
                log.info(f"Datas em resource.created")
    elif len(dates[0]) == 1:
        date = dates[0][0]
        interval = (date[0], date[0] + relativedelta(months=date[1] - 1, day=31)) \
            if date[1] != 0 else (date[0], date[0])
    else:
        dates = [(date[0], date[0] + relativedelta(months=date[1] - 1, day=31))
                 if date[1] != 0
                 else (date[0], date[0])
                 for date in dates[0]]
        least_recent = dates[0][0]
        last = dates[0][1]
        for date in dates[1:]:
            if date[0] < least_recent:
                least_recent = date[0]
            if date[1] > last:
                last = date[1]
        interval = [least_recent, last]

    if interval:
        interval = [i.date() for i in interval]
        if resource.updated:
            log.info(f"recurso {resource.id} marcado para atualização")
            delete_temporal_index(resource.id)

        insert_index(resource.id, interval[0], interval[1], resource.package_id, num_resources_package)
    log.info(f"intervalo encontrado: {interval}")


def run_temporal_dataset_indexing(dataset: MetadataDataset, update_num_package_resources):
    indexed_resources_ids = []
    if update_num_package_resources:
        update_num_resources(dataset.id, dataset.num_resources)
    for resource in dataset.resources:
        log.info('id_resource: ' + resource.id)
        run_temporal_index(resource, dataset.num_resources, dataset.title, dataset.notes)
        indexed_resources_ids.append(resource.id)
    return indexed_resources_ids


def delete_temporal_indexes(dataset: MetadataDataset):
    for resource in dataset.resources:
        delete_temporal_index(resource.id)
    update_num_resources(dataset.id, dataset.num_resources)
