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

import java.io.IOException;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.nosto.redis.RedisClusterConnector;
import com.nosto.redis.SingleNodeRedisConnector;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;

/**
 * Extend this class to run tests against a single Redis node and a Redis cluster.
 */
@RunWith(Parameterized.class)
public abstract class AbstractScriptTest {
    private static final String REDIS_SINGLE = "redissingle";
    private static final String REDIS_CLUSTER = "rediscluster";

    @ClassRule
    public static final DockerComposeRule DOCKER_RULE = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .build();

    @Parameterized.Parameter
    public String parameterName;

    protected AbstractScript script;

    private SingleNodeRedisConnector singleNodeRedisConnector;
    private RedisClusterConnector redisClusterConnector;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] {"redissingle"};
    }

    @Before
    public void setUp() throws IOException, InterruptedException {
        DockerPort servicePort = DOCKER_RULE.dockerCompose()
                .ports(parameterName)
                .stream()
                .findFirst()
                .get();

        if (REDIS_SINGLE.equals(parameterName)) {
            singleNodeRedisConnector = new SingleNodeRedisConnector(servicePort.getIp(), servicePort.getExternalPort());
            singleNodeRedisConnector.flush();
            script = new SingleNodeScript(singleNodeRedisConnector.getJedisPool(), 0);
        } else if (REDIS_CLUSTER.equals(parameterName)) {
            redisClusterConnector = new RedisClusterConnector(servicePort.getIp(), servicePort.getExternalPort());
            redisClusterConnector.flush();
            script = new ClusterScript(redisClusterConnector.getJedisCluster(), 12);
        } else {
            throw new IllegalStateException("Unknown parameter: " + parameterName);
        }
    }
}
