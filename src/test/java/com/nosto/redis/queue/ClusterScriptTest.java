/*******************************************************************************
 * Copyright (c) 2018 Nosto Solutions Ltd All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of
 * Nosto Solutions Ltd ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the agreement you entered into with
 * Nosto Solutions Ltd.
 ******************************************************************************/

package com.nosto.redis.queue;

import com.nosto.redis.RedisClusterConnector;
import org.junit.Before;
import org.junit.ClassRule;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;

public class ClusterScriptTest extends AbstractScriptTest {
    @ClassRule
    public static RedisClusterConnector jedis = new RedisClusterConnector();

    public ClusterScriptTest() throws IOException {
        super(new ClusterScript(jedis.getJedisCluster(), 12));
    }

    @Before
    public void flush() {
        jedis.getJedisCluster().getClusterNodes().forEach((key, jedisPool) -> {
            try (Jedis j = jedisPool.getResource()) {
                j.flushDB();
            } catch (JedisDataException e) {
                // redis.clients.jedis.exceptions.JedisDataException: READONLY You can't write against a read only slave.
            }
        });
    }


}