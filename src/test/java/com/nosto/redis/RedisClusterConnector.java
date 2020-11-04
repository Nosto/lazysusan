/******************************************************************************
 Copyright (c) 2019 Nosto Solutions Ltd All Rights Reserved.
 <p>
 This software is the confidential and proprietary information of
 Nosto Solutions Ltd ("Confidential Information"). You shall not
 disclose such Confidential Information and shall use it only in
 accordance with the terms of the agreement you entered into with
 Nosto Solutions Ltd.
 */
package com.nosto.redis;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisClusterConnector implements RedisConnector {
    private final JedisCluster jedisCluster;

    public RedisClusterConnector(String host, int port) {
        jedisCluster = new JedisCluster(new HostAndPort(host, port));
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
                // redis.clients.jedis.exceptions.JedisDataException: READONLY You can't write against a read only slave
            }
        });
    }
}
