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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class SingleNodeRedisConnector extends ExternalResource implements RedisConnector {
    private final String host;
    private final int port;
    private final JedisPool jedisPool;

    public SingleNodeRedisConnector(String host, int port) {
        this.host = host;
        this.port = port;
        jedisPool = new JedisPool(host, port);
    }

    public JedisPool getJedisPool() {
        return jedisPool;
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
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.flushDB();
        }
    }
}
