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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.IntStream;

public abstract class RedisConnector {
    protected static final Logger logger = LoggerFactory.getLogger(RedisConnector.class);

    private static final int TRIALS_TO_WAIT_STARTUP = 20;

    protected abstract boolean isAlive();

    public abstract RedisConnector flush();

    public abstract AbstractScript buildRedisScript(DequeueStrategy dequeueStrategy);

    public final RedisConnector waitToStartUp(String dockerService) {
        IntStream.range(0, TRIALS_TO_WAIT_STARTUP)
                .filter(this::waitIfNotAlive)
                .findFirst()
                .ifPresentOrElse(
                        round -> logger.info("Service " + dockerService + " started on trial " + round),
                        () -> {
                            throw new IllegalStateException("Failed to start service " + dockerService);
                        });
        return this;
    }

    private boolean waitIfNotAlive(int round) {
        boolean alive = isAlive();
        if (!alive) {
            try {
                logger.info("Trial " + round + ": Redshift not alive yet. Wait for awhile.");
                Thread.sleep(1000);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return alive;
    }
}
