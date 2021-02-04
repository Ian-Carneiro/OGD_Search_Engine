FROM neo4j:4.2

ENV NEO4J_AUTH=neo4j/j4oen
COPY csv/place_neo4j.csv /var/lib/neo4j/import/place_neo4j.csv
COPY csv/siglas_uf.csv /var/lib/neo4j/import/siglas_uf.csv
COPY init_db.cql init_db.cql
COPY init_db.sh init_db.sh
COPY wrapper.sh wrapper.sh
ENTRYPOINT ["./wrapper.sh"]
