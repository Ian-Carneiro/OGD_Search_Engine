FROM postgres:10
ENV POSTGRES_DB data_processor_db
ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD postgres

ENV POSTGIS_MAJOR 2.4

RUN apt-get update \
      && apt-cache showpkg postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
      && apt-get install -y --no-install-recommends\
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR\
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts\
      && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /docker-entrypoint-initdb.d
COPY ./initdb-postgis.sh /docker-entrypoint-initdb.d/postgis.sh

