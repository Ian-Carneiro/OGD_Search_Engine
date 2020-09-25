#!/bin/bash

until cypher-shell -u neo4j -p j4oen -f ./init_db.cql
do
  echo "init database failed, sleeping"
  sleep 10
done
