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

import java.util.stream.IntStream;

// TODO: Convert to abstract class and hide internal implementation from outsiders
// TODO: Move to same package as redis script classes to avoid visibility changes
public interface RedisConnector {
    boolean isAlive();

    RedisConnector flush();

    AbstractScript buildRedisScript(DequeueStrategy dequeueStrategy);

    default RedisConnector waitToStartUp(String dockerService) {
        int trials = 5;
        if (IntStream.range(0, trials).filter(r -> waitIfNotAlive()).findFirst().isEmpty()) {
            throw new IllegalStateException("Failed to start service " + dockerService);
        }
        return this;
    }

    default boolean waitIfNotAlive() {
        boolean alive = isAlive();
        if (!alive) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return alive;
    }

}
