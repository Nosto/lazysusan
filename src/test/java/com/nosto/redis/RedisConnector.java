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

import com.nosto.redis.queue.AbstractScript;
import com.nosto.redis.queue.DequeueStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.stream.IntStream;

// TODO: Convert to abstract class and hide internal implementation from outsiders
// TODO: Move to same package as redis script classes to avoid visibility changes
public abstract class RedisConnector {
    protected static final Logger logger = LoggerFactory.getLogger(RedisConnector.class);

    protected abstract boolean isAlive();

    public abstract RedisConnector flush();

    public abstract AbstractScript buildRedisScript(DequeueStrategy dequeueStrategy);

    public RedisConnector waitToStartUp(String dockerService) {
        int trials = 5;
        if (IntStream.range(0, trials).filter(this::waitIfNotAlive).findFirst().isEmpty()) {
            IllegalStateException e = new IllegalStateException("Failed to start service " + dockerService);
            logger.error("waitToStartUp failed", e);
            System.out.println("=== waitToStartUp failed");
            e.printStackTrace();
            throw e;
        }
        return this;
    }

    protected boolean ping(JedisPool jedisPool) {
        try (Jedis jedis = jedisPool.getResource()) {
            String pingResponse = jedis.ping();
            System.out.println("=== pingResponse: " + pingResponse);
            boolean alive = "PONG".equals(pingResponse);
            if (!alive) {
                logger.error("Redis not alive. Response: " + pingResponse);
                System.out.println("=== Redis not alive. Response: " + pingResponse);
            }
            return alive;
        } catch (Exception e) {
            logger.error("Redis ping failed.", e);
            System.out.println("=== Redis ping failed.");
            e.printStackTrace();
            return false;
        }
    }

    private boolean waitIfNotAlive(int round) {
        boolean alive = isAlive();
        if (!alive) {
            try {
                logger.info("Trial " + round + ": Redshift not alive yet. Wait for awhile.");
                System.out.println("=== Trial " + round + ": Redshift not alive yet. Wait for awhile.");
                Thread.sleep(1000);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return alive;
    }

}
