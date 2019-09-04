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

@RunWith(Parameterized.class)
public abstract class AbstractScriptTest {
    @ClassRule
    @Rule
    public static RedisClusterConnector jedisCluster = new RedisClusterConnector();

    @ClassRule
    @Rule
    public static SingleNodeRedisConnector singleJedis = new SingleNodeRedisConnector();

    @Parameterized.Parameter
    public AbstractScript script;

    @Parameterized.Parameters
    public static Collection scripts() throws IOException {
        return List.of(
                new SingleNodeScript(singleJedis.getJedis()),
                new ClusterScript(jedisCluster.getJedisCluster(), 12));
    }
}
