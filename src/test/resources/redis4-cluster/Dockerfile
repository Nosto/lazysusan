FROM redis:4.0

RUN apt-get update
RUN apt-get -y install ruby wget
RUN gem install redis -v 4.0.0

RUN wget http://download.redis.io/releases/redis-4.0.0.tar.gz -O /data/redis.tar.gz && \
    tar xzf /data/redis.tar.gz -C /data && \
    mv /data/redis-4.0.0 /data/redis
