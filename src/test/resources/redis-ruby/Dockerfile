ARG img_version
FROM redis:${img_version}

ARG src_version
RUN apt-get update
RUN apt-get -y install ruby wget

ARG gem_version
RUN gem install redis -v ${gem_version}

WORKDIR /redis-src
RUN wget https://download.redis.io/releases/redis-${src_version}.tar.gz && \
    tar xzf redis-${src_version}.tar.gz && \
    mv redis-${src_version} redis
