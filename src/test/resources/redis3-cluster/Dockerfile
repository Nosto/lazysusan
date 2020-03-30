FROM redis:3.0

RUN apt-get update
RUN apt-get -y install ruby wget
RUN gem install redis -v 3.0.0

WORKDIR /redis-src
RUN wget http://download.redis.io/releases/redis-3.0.0.tar.gz && \
    tar xzf redis-3.0.0.tar.gz && \
    mv redis-3.0.0 redis
