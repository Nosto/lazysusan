#!/bin/sh

NODES=""
for PORT in "$@"; do
  redis-server --port $PORT --cluster-enabled yes --cluster-config-file nodes-$PORT.conf \
   --cluster-node-timeout 5000 --appendonly yes --appendfilename appendonly-$PORT.aof \
   --dbfilename dump-$PORT.rdb --logfile $PORT.log --daemonize yes

   NODES="$NODES 127.0.0.1:$PORT"
done

echo "yes" | eval redis-cli --cluster create $NODES --cluster-replicas 1

tail -f $1.log