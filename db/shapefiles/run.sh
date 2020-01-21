ogr2ogr -sql "select nome, 'REGIÃO' as tipo from macroregiões" ./modified_shapefiles/1.shp macroregiões.shp;
ogr2ogr -sql "select nome, 'UF' as tipo from lml_unidade_federacao_a" ./modified_shapefiles/2.shp lml_unidade_federacao_a.shp;
ogr2ogr -sql "select nome, 'MUNICÍPIO' as tipo from lml_municipio_a" ./modified_shapefiles/3.shp lml_municipio_a.shp;
shp2pgsql ./modified_shapefiles/1.shp place > ./sql/1.sql;
shp2pgsql -a ./modified_shapefiles/2.shp place > ./sql/2.sql;
shp2pgsql -a ./modified_shapefiles/3.shp place > ./sql/3.sql;
psql -U postgres -d data_processor_db -p 5433 -h localhost -a -f "./sql/1.sql";
psql -U postgres -d data_processor_db -p 5433 -h localhost -a -f "./sql/2.sql";
psql -U postgres -d data_processor_db -p 5433 -h localhost -a -f "./sql/3.sql";


