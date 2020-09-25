db_connection = "postgresql+psycopg2://postgres:postgres@metadata_database:5432/metadata_processor_db"

log = {
    "max_file_bytes": 1024*1024,
    "backup_count": 5,
    "file_name": "./logs/data_processor.log"
}

scheduled_hour = 1

num_dataset_per_iteration = 1
