from os import cpu_count

db_connection = {
    "bolt_uri": "bolt://spatial_index_database:7687",
    "user": "neo4j",
    "password": "j4oen"
}

num_lines_to_check_csv_dialect = 50

num_lines_to_check_type_of_place = 100

num_cpu_to_index = cpu_count()

csv_chunk_size = 1024 * 1024 * 50  # 50MB

request_timeout = (5, 36)


