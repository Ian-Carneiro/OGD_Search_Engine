import datetime
from time import sleep
from index_log import log
from model import get_dataset, spatial_indexing_done, temporal_indexing_done, thematic_indexing_done, \
    get_deleted_resources
from spatial_indexing.spatial_indexing import run_spatial_dataset_indexing, delete_spatial_indexes
from temporal_indexing.temporal_indexing import run_temporal_dataset_indexing, delete_temporal_indexes
from thematic_indexing.thematic_indexing import run_thematic_dataset_indexing, delete_thematic_indexes
from metadata_processor.metadata_processor import download_and_persist_metadata


def remove():
    log.info("Removendo recursos que não pertencem mais a base de dados.")
    while True:
        packages = get_deleted_resources()
        if not packages:
            break
        for package in packages:
            delete_spatial_indexes(package)
            delete_temporal_indexes(package)
            delete_thematic_indexes(package)


def index(update_hour, current_date, num=0):
    log.info("Indexando recursos...")
    while True:
        packages = get_dataset(num)
        now = datetime.datetime.now()
        if now.hour == update_hour and now.date() > current_date:
            log.info("Horário de atualização dos metadados.")
            return False
        if not packages:
            log.info("Todos os recursos foram atualizados.")
            return True
        for package in packages:
            log.info('#----------------------------------------------------------------------------------------------#')
            log.info(str(num) + ' - ' + package.id)
            resources = package.resources
            if resources:
                log.info('')
                log.info('#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>indexação espacial<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<#')
                package.resources = [resource for resource in resources if not resource.spatial_indexing]
                update_num_package_resources = False
                if len(package.resources) < len(resources):
                    update_num_package_resources = True
                spatial_indexing_done(run_spatial_dataset_indexing(package, update_num_package_resources))

                log.info('')
                log.info('#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>indexação temporal<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<#')
                package.resources = [resource for resource in resources if not resource.temporal_indexing]
                update_num_package_resources = False
                if len(package.resources) < len(resources):
                    update_num_package_resources = True
                temporal_indexing_done(run_temporal_dataset_indexing(package, update_num_package_resources))

                log.info('')
                log.info('#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>indexação temática<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<#')
                package.resources = [resource for resource in resources if not resource.thematic_indexing]
                thematic_indexing_done(run_thematic_dataset_indexing(package))
            num += 1


def task(task_hour=15):
    while True:
        day = datetime.datetime.now()
        download_and_persist_metadata()
        remove()
        finish = index(task_hour, day.date())
        if finish:
            print('finish')
            while datetime.datetime.now().hour != task_hour:
                sleep(1)


task()
