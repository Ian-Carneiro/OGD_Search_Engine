import datetime

db_connection = "postgresql+psycopg2://postgres:postgres@temporal_index_database:5432/temporal_indexing_db"

min_date_allowed = datetime.datetime(1900, 1, 1)

max_date_allowed = datetime.datetime.now()
