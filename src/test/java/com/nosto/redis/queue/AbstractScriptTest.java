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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.nosto.redis.RedisClusterConnector;
import com.nosto.redis.SingleNodeRedisConnector;

/**
 * Extend this class to run tests against a single Redis node and a Redis cluster.
 */
@RunWith(Parameterized.class)
public abstract class AbstractScriptTest {
    @ClassRule
    @Rule
    public static RedisClusterConnector jedisCluster = new RedisClusterConnector();

    @ClassRule
    @Rule
    public static SingleNodeRedisConnector singleJedis = new SingleNodeRedisConnector();

    @Parameterized.Parameter(1)
    public AbstractScript script;

    @Parameterized.Parameter
    public String parameterName;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> scripts() throws IOException {
        return List.of(new Object[][] {
                { "single", new SingleNodeScript(singleJedis.getJedis()) },
                { "cluster", new ClusterScript(jedisCluster.getJedisCluster(), 12)}});
    }
}
