/*******************************************************************************
 * Copyright (c) 2019 Nosto Solutions Ltd All Rights Reserved.
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

public class RedisClusterConnector extends ExternalResource implements RedisConnector {
    private final String host;
    private final int port;
    private final JedisCluster jedisCluster;

    public RedisClusterConnector(String host, int port) {
        this.host = host;
        this.port = port;
        jedisCluster = new JedisCluster(new HostAndPort(host, port), 1000);
    }

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void flush() {
        jedisCluster.getClusterNodes().forEach((key, jedisPool) -> {
            try (Jedis j = jedisPool.getResource()) {
                j.flushDB();
            } catch (JedisDataException e) {
                // redis.clients.jedis.exceptions.JedisDataException: READONLY You can't write against a read only slave
            }
        });
    }
}
