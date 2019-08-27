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
import redis.clients.jedis.Jedis;

public class SingleNodeRedisConnector extends ExternalResource {
    private final Jedis jedis;

    public SingleNodeRedisConnector() {
        jedis = new Jedis("redis.dev.nos.to", 6379);
    }

    public Jedis getJedis() {
        return jedis;
    }
}
