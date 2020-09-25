model_config = {
    "db_connection": "postgresql+psycopg2://postgres:postgres@metadata_database:5432/metadata_processor_db"
}

spatial_config = {
    "db_connection": {
        "bolt_uri": "bolt://spatial_index_database:7687",
        "user": "neo4j",
        "password": "j4oen"
    }
}

temporal_config = {
    "db_connection": "postgresql+psycopg2://postgres:postgres@temporal_index_database:5432/temporal_indexing_db",
    "alpha_value": 0.5,
    "beta_value": 0.5
}

thematic_config = {
    "resource_solr_core_uri": "http://thematic_index_solr:8983/solr/resource",
    "dataset_solr_core_uri": "http://thematic_index_solr:8983/solr/dataset"
}

