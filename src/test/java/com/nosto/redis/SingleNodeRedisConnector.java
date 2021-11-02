/*
 *  Copyright (c) 2020 Nosto Solutions Ltd All Rights Reserved.
 *
 *  This software is the confidential and proprietary information of
 *  Nosto Solutions Ltd ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the agreement you entered into with
 *  Nosto Solutions Ltd.
 */
package com.nosto.redis;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
public class SingleNodeRedisConnector implements RedisConnector {
    private final JedisPool jedisPool;

    public SingleNodeRedisConnector(String host, int port) {
        jedisPool = new JedisPool(host, port);
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    @Override
    public void flush() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.flushDB();
        }
    }
}
