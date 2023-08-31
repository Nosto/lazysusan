/*
 *  Copyright (c) 2020 Nosto Solutions Ltd All Rights Reserved.
 *
 *  This software is the confidential and proprietary information of
 *  Nosto Solutions Ltd ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the agreement you entered into with
 *  Nosto Solutions Ltd.
 */
package com.nosto.redis.queue;

import com.palantir.docker.compose.connection.Container;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableMap;
import com.nosto.redis.RedisClusterConnector;
import com.nosto.redis.SingleNodeRedisConnector;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;

/**
 * Extend this class to run tests against a single Redis node and a Redis cluster.
 */
@RunWith(Parameterized.class)
public abstract class AbstractScriptTest {
    @ClassRule
    public static final DockerComposeRule DOCKER_RULE = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .build();
    // Names of docker services to connect to and a flag to denote if the container is a single node redis instance.
    private static final ImmutableMap<String, Boolean> CONTAINERS = ImmutableMap.<String, Boolean>builder()
            .put("redis6single", true)
            .put("redis6cluster", false)
            .put("redis7single", true)
            .put("redis7cluster", false)
            .build();
    @Parameterized.Parameter
    public String dockerService;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return CONTAINERS.keySet().toArray();
    }

    private Container container;

    @SuppressWarnings("NullAway")
    @Before
    public void setUp() throws Throwable {
        container = DOCKER_RULE.containers().container(dockerService);
        container.start();
    }

    protected AbstractScript buildScript(DequeueStrategy dequeueStrategy) {
        DockerPort servicePort = container
                .ports()
                .stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No port defined"));
        if (isSingleNode()) {
            SingleNodeRedisConnector singleNodeRedisConnector =
                    new SingleNodeRedisConnector(servicePort.getIp(), servicePort.getExternalPort());
            singleNodeRedisConnector.flush();
            return new SingleNodeScript(singleNodeRedisConnector.getJedisPool(), 0, dequeueStrategy);
        } else {
            RedisClusterConnector redisClusterConnector =
                    new RedisClusterConnector(servicePort.getIp(), servicePort.getExternalPort());
            redisClusterConnector.flush();
            return new ClusterScript(redisClusterConnector.getJedisCluster(), 12, dequeueStrategy);
        }
    }

    protected boolean isSingleNode() {
        return CONTAINERS.get(dockerService);
    }
}
