#!/bin/sh

for PORT in 7000 7001 7002 7003 7004 7005; do
  redis-server --port $PORT --cluster-enabled yes --cluster-config-file nodes-$PORT.conf \
   --cluster-node-timeout 1000 --appendonly yes --appendfilename appendonly-$PORT.aof \
   --dbfilename dump-$PORT.rdb --logfile $PORT.log --daemonize yes
done

echo "yes" | eval redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 \
127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 \
--cluster-replicas 1

tail -f 7000.log