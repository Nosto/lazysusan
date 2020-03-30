#!/bin/sh

REDIS_VERSION=$1
shift

NODES=""
for PORT in "$@"; do
  redis-server --port $PORT --cluster-enabled yes --cluster-config-file nodes-$PORT.conf \
   --cluster-node-timeout 5000 --appendonly yes --appendfilename appendonly-$PORT.aof \
   --dbfilename dump-$PORT.rdb --logfile $PORT.log --daemonize yes

   NODES="$NODES 127.0.0.1:$PORT"
done

apt-get update
apt-get -y install ruby wget

cd /data
wget http://download.redis.io/releases/redis-$REDIS_VERSION.tar.gz
tar xzf redis-$REDIS_VERSION.tar.gz

gem install redis -v $REDIS_VERSION
echo "yes" | eval ruby /data/redis-$REDIS_VERSION/src/redis-trib.rb create --replicas 1 $NODES

tail -f $1.log