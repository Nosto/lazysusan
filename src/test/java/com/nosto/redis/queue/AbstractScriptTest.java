/*******************************************************************************
 * Copyright (c) 2019 Nosto Solutions Ltd All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of
 * Nosto Solutions Ltd ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the agreement you entered into with
 * Nosto Solutions Ltd.
 ******************************************************************************/
package com.nosto.redis.queue;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.nosto.redis.RedisClusterConnector;
import com.nosto.redis.RedisConnector;
import com.nosto.redis.SingleNodeRedisConnector;

/**
 * Extend this class to run tests against a single Redis node and a Redis cluster.
 */
@RunWith(Parameterized.class)
public abstract class AbstractScriptTest {
    @ClassRule
    public static RedisClusterConnector jedisCluster = new RedisClusterConnector();

    @ClassRule
    public static SingleNodeRedisConnector singleJedis = new SingleNodeRedisConnector();

    @Parameterized.Parameter
    public String parameterName;

    @Parameterized.Parameter(1)
    public RedisConnector redisConnector;

    @Parameterized.Parameter(2)
    public AbstractScript script;


    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> scripts() {
        return Arrays.asList(new Object[][] {
                {"single", singleJedis, new SingleNodeScript(singleJedis.getJedisPool(), 0)},
                {"cluster", jedisCluster, new ClusterScript(jedisCluster.getJedisCluster(), 12)}});
    }

    @Before
    public void setUp() {
        redisConnector.flush();
    }
}
