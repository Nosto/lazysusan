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
import redis.clients.jedis.JedisCluster;

public class RedisClusterConnector extends ExternalResource {
    private final JedisCluster jedisCluster;

    public RedisClusterConnector() {
        HostAndPort hostAndPort = new HostAndPort("rediscluster.dev.nos.to", 7100);
        jedisCluster = new JedisCluster(hostAndPort, 1000);
    }

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }
}
