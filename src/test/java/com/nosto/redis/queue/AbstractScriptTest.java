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

import java.io.File;
import java.util.Objects;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.DockerComposeContainer;

import com.nosto.redis.RedisClusterConnector;
import com.nosto.redis.SingleNodeRedisConnector;

/**
 * Extend this class to run tests against a single Redis node and a Redis cluster.
 */
@RunWith(Parameterized.class)
public abstract class AbstractScriptTest {
    @ClassRule
    public static final SingleNodeRedisConnector REDIS_SINGLE_CONNECTOR =
            new SingleNodeRedisConnector("redissingle.dev.nos.to", 6379);

    @ClassRule
    public static final RedisClusterConnector REDIS_CLUSTER_CONNECTOR =
            new RedisClusterConnector("rediscluster.dev.nos.to", 7100);

    @ClassRule
    public static final DockerComposeContainer DOCKER_COMPOSE_CONTAINER =
            new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"));

    @Parameterized.Parameter
    public String parameterName;

    protected AbstractScript script;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] {REDIS_SINGLE_CONNECTOR.getHost()};
    }

    @Before
    public void setUp() {
        if (Objects.equals(parameterName, REDIS_SINGLE_CONNECTOR.getHost())) {
            REDIS_SINGLE_CONNECTOR.flush();
            script = new SingleNodeScript(REDIS_SINGLE_CONNECTOR.getJedisPool(), 0);
        } else if (Objects.equals(parameterName, REDIS_CLUSTER_CONNECTOR.getHost())) {
            REDIS_CLUSTER_CONNECTOR.flush();
            script = new ClusterScript(REDIS_CLUSTER_CONNECTOR.getJedisCluster(), 12);
        } else {
            throw new IllegalStateException("Unknown parameter: " + parameterName);
        }
    }
}
