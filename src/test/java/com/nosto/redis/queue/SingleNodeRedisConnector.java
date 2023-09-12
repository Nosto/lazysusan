/*
 *  Copyright (c) 2023 Nosto Solutions Ltd All Rights Reserved.
 *
 *  This software is the confidential and proprietary information of
 *  Nosto Solutions Ltd ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the agreement you entered into with
 *  Nosto Solutions Ltd.
 */
package com.nosto.redis.queue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
public class SingleNodeRedisConnector extends RedisConnector {
    private final JedisPool jedisPool;

    public SingleNodeRedisConnector(String host, int port) {
        jedisPool = new JedisPool(host, port);
    }

    @Override
    public boolean isAlive() {
        try (Jedis jedis = jedisPool.getResource()) {
            String pingResponse = jedis.ping();
            boolean alive = "PONG".equals(pingResponse);
            if (!alive) {
                logger.info("Redis not alive. Response: " + pingResponse);
            }
            return alive;
        } catch (Exception e) {
            logger.error("Redis ping failed.", e);
            return false;
        }
    }

    @Override
    public RedisConnector flush() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.flushDB();
        }
        return this;
    }

    @Override
    public AbstractScript buildRedisScript(DequeueStrategy dequeueStrategy) {
        return new SingleNodeScript(jedisPool, 0, dequeueStrategy);
    }
}
