LOAD CSV WITH HEADERS FROM 'file:///place_neo4j.csv' AS row
create(:Place{gid: row.gid, nome: row.nome, tipo: row.tipo, fk: row.fk});

match(p1:Place {tipo:"UF"}),(p2:Place {tipo:"REGIÃO"})
where p1.fk=p2.gid
create (p2)-[:_CONTAINS]->(p1);

match(p1:Place {tipo:"MUNICÍPIO"}),(p2:Place {tipo:"UF"})
where p1.fk=p2.gid
create (p2)-[:_CONTAINS]->(p1);

create constraint on (r:Resource) assert r.id is unique;
create constraint on (p:Place) assert p.gid is unique;

LOAD CSV WITH HEADERS FROM 'file:///siglas_uf.csv' AS row
match(p:Place{nome:row.uf, tipo:"UF"})
set p.sigla=row.sigla


