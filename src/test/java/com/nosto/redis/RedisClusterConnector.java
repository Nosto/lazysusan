/*******************************************************************************
 * Copyright (c) 2018 Nosto Solutions Ltd All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of
 * Nosto Solutions Ltd ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the agreement you entered into with
 * Nosto Solutions Ltd.
 ******************************************************************************/
package com.nosto.redis;

import org.junit.rules.ExternalResource;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisClusterConnector extends ExternalResource implements RedisConnector{
    private final JedisCluster jedisCluster;

    public RedisClusterConnector() {
        HostAndPort hostAndPort = new HostAndPort("rediscluster.dev.nos.to", 7100);
        jedisCluster = new JedisCluster(hostAndPort, 1000);
    }

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    @Override
    public void flush() {
        jedisCluster.getClusterNodes().forEach((key, jedisPool) -> {
            try (Jedis j = jedisPool.getResource()) {
                j.flushDB();
            } catch (JedisDataException e) {
                // redis.clients.jedis.exceptions.JedisDataException: READONLY You can't write against a read only slave.
            }
        });
    }
}
