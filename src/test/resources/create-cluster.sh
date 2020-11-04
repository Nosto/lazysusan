#!/bin/sh

NODES=""
for PORT in $(seq "$1" "$2"); do
  redis-server --port "$PORT" --cluster-enabled yes --cluster-config-file nodes-"$PORT".conf \
   --cluster-node-timeout 5000 --appendonly yes --appendfilename appendonly-"$PORT".aof \
   --dbfilename dump-"$PORT".rdb --logfile "$PORT".log --daemonize yes

   NODES="$NODES 127.0.0.1:$PORT"
done

if [ -f /redis-src/redis/src/redis-trib.rb ]; then
  echo "yes" | eval ruby /redis-src/redis/src/redis-trib.rb create --replicas 1 "$NODES"
else
  echo "yes" | eval redis-cli --cluster create "$NODES" --cluster-replicas 1
fi

tail -f "$1".log
