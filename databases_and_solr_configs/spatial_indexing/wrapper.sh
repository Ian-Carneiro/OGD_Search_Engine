#!/bin/bash

# turn on bash's job control
set -m

# Start the primary process and put it in the background
/docker-entrypoint.sh neo4j &

# Start the helper process
./init_db.sh
echo "echo 'Grafo de lugares já foi persistido.'" > ./init_db.sh
# the my_helper_process might need to know how to wait on the
# primary process to start before it does its work and returns


# now we bring the primary process back into the foreground
# and leave it there
fg %1
