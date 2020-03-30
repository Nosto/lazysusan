#!/bin/sh

if "$1" == "yes"; then
  CREATE_CLUSTER=true
fi

shift

NODES=""
for PORT in "$@"; do
  redis-server --port $PORT --cluster-enabled yes --cluster-config-file nodes-$PORT.conf \
   --cluster-node-timeout 1000 --appendonly yes --appendfilename appendonly-$PORT.aof \
   --dbfilename dump-$PORT.rdb --logfile $PORT.log --daemonize yes
   NODES="$NODES 127.0.0.1:$PORT"
done

if $CREATE_CLUSTER; then
  echo "yes" | eval redis-cli --cluster create $NODES --cluster-replicas 1
fi

tail -f $1.log