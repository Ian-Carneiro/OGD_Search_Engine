FROM postgres:10
ENV POSTGRES_DB metadata_processor_db
ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD postgres
ENV POSTGRES_PORT 5432
COPY metadata_processor.sql /docker-entrypoint-initdb.d/
