import datetime
from time import sleep
from index_log import log
from model import get_dataset, spatial_indexing_done, temporal_indexing_done, thematic_indexing_done, \
    get_deleted_resources
from spatial_indexing.spatial_indexing import run_spatial_dataset_indexing, delete_spatial_indexes
from temporal_indexing.temporal_indexing import run_temporal_dataset_indexing, delete_temporal_indexes
from thematic_indexing.thematic_indexing import run_thematic_dataset_indexing, delete_thematic_indexes
from metadata_processor.metadata_processor import download_and_persist_metadata
from config import config
import connections_and_database_test

connections_and_database_test.run()


def remove():
    log.info('#----------------------------------------------------------------------------------------------#')
    log.info("Removendo recursos que não pertencem mais a base de dados.")
    while True:
        results = get_deleted_resources()
        if not results:
            break
        for result in results:
            delete_spatial_indexes(result[0], result[1])
            delete_temporal_indexes(result[0], result[1])
            delete_thematic_indexes(result[1])


def index(update_hour, current_date, num=0):
    log.info('#----------------------------------------------------------------------------------------------#')
    log.info("Indexando recursos...")
    while True:
        packages = get_dataset(num)
        now = datetime.datetime.now()
        if now.hour == update_hour and now.date() > current_date:
            log.info("Parando indexação para atualizar metadados.")
            return False
        if not packages:
            log.info("Todos os recursos foram atualizados hoje.")
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
                if not package.temporal_indexing:
                    package.resources = resources
                    temporal_indexing_done(run_temporal_dataset_indexing(package, False), package.id)
                else:
                    package.resources = [resource for resource in resources if not resource.temporal_indexing]
                    update_num_package_resources = False
                    if len(package.resources) < len(resources):
                        update_num_package_resources = True
                    temporal_indexing_done(run_temporal_dataset_indexing(package, update_num_package_resources))
                log.info('')
                log.info('#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>indexação temática<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<#')
                if not package.thematic_indexing:
                    package.resources = resources
                    thematic_indexing_done(run_thematic_dataset_indexing(package), package.id)
                for resource in resources:
                    if not resource.thematic_indexing:
                        package.resources = resources
                        thematic_indexing_done(run_thematic_dataset_indexing(package))
                        break
            num += 1


def task(task_hour=config.scheduled_hour):
    while True:
        day = datetime.datetime.now()
        download_and_persist_metadata()
        remove()
        finish = index(task_hour, day.date())
        if finish:
            day = datetime.datetime.now().date()
            log.info("Amanhã os metadados serão atualizados.")
            while datetime.datetime.now().hour != task_hour and day == datetime.datetime.now().date():
                sleep(1)


task()
