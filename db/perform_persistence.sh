ogr2ogr -sql "select nome, 'REGIÃO' as tipo from macroregiões" ./shapefiles/modified_shapefiles/1.shp ./shapefiles/macroregiões.shp;
ogr2ogr -sql "select nome, 'UF' as tipo from lml_unidade_federacao_a" ./shapefiles/modified_shapefiles/2.shp ./shapefiles/lml_unidade_federacao_a.shp;
ogr2ogr -sql "select nome, 'MUNICÍPIO' as tipo from lml_municipio_a" ./shapefiles/modified_shapefiles/3.shp ./shapefiles/lml_municipio_a.shp;

shp2pgsql ./shapefiles/modified_shapefiles/1.shp place > ./shapefiles/sql/1.sql;
shp2pgsql -a ./shapefiles/modified_shapefiles/2.shp place > ./shapefiles/sql/2.sql;
shp2pgsql -a ./shapefiles/modified_shapefiles/3.shp place > ./shapefiles/sql/3.sql;

PGPASSWORD=postgres psql -U postgres -d data_processor_db -p 5433 -h localhost -a -f "./shapefiles/sql/1.sql";
PGPASSWORD=postgres psql -U postgres -d data_processor_db -p 5433 -h localhost -a -f "./shapefiles/sql/2.sql";
PGPASSWORD=postgres psql -U postgres -d data_processor_db -p 5433 -h localhost -a -f "./shapefiles/sql/3.sql";

PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d data_processor_db -c \
"create table place_neo4j(               
        gid serial,
        nome text,
        tipo text,
        primary key(gid),
        fk int
);"

PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d data_processor_db -c \
"insert into place_neo4j
select p1.gid, p1.nome, p1.tipo, null as fk
from place p1 
where p1.tipo like 'REGIÃO';"

PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d data_processor_db -c \
"insert into place_neo4j
select p2.gid, p2.nome, p2.tipo, p1.gid as fk 
from place p1, place p2 
where st_contains(p1.geom, p2.geom) and p1.gid <> p2.gid 
	and (p1.tipo like 'REGIÃO' and p2.tipo like 'UF');"

PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d data_processor_db -c \
"insert into place_neo4j
select p2.gid, p2.nome, p2.tipo, p1.gid as fk 
from place p1, place p2 
where st_contains(p1.geom, p2.geom) and p1.gid <> p2.gid 
	and (p1.tipo like 'UF' and p2.tipo like 'MUNICÍPIO');"

PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d data_processor_db -c \
"ALTER TABLE place_neo4j 
ADD CONSTRAINT FK_place_neo4j
FOREIGN KEY (fk)
REFERENCES place_neo4j(gid);"

PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d data_processor_db -A -F "," -c "Select * From place_neo4j;" > ./neo4j/csv/place_neo4j.csv

PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d data_processor_db -c "drop table place_neo4j"

echo "CONSTRUINDO GRAFO DE LOCAIS \n"

echo "... \n"

cat ./neo4j/persist_neo4j.cql | sudo docker exec --interactive neo4j_tcc bin/cypher-shell 

echo "GRAFO DOS LOCAIS CONSTRUÍDO"























