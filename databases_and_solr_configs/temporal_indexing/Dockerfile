FROM postgres
ENV POSTGRES_DB temporal_indexing_db
ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD postgres
ENV POSTGRES_PORT 5432
COPY temporal_indexing.sql /docker-entrypoint-initdb.d/
