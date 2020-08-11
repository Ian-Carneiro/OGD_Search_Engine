# import metadata_processor.metadata_processor
from index_log import log
from model import get_dataset
from spatial_indexing.spatial_indexing import run_spatial_dataset_indexing
from temporal_indexing.temporal_indexing import run_temporal_dataset_indexing
from thematic_indexing.thematic_indexing import run_thematic_dataset_indexing


quant = 516 #5024
while True:
    datasets = get_dataset(quant)
    if not datasets:
        break
    for dataset in datasets:
        log.info('#--------------------------------------------------------------------------------------------------#')
        log.info(str(quant) + ' - ' + dataset.id)
        run_spatial_dataset_indexing(dataset)
        # run_temporal_dataset_indexing(dataset)
        # run_thematic_dataset_indexing(dataset)
        quant += 1
