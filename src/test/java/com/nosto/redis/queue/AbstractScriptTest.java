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

import com.nosto.redis.RedisConnector;
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

    protected static DockerComposeRule dockerComposeRule() {
        return DockerComposeRule.builder()
                .file("src/test/resources/docker-compose.yml")
                .build();
    }

    protected AbstractScript buildScript(DockerComposeRule dockerComposeRule, DequeueStrategy dequeueStrategy) {
        try {
            DockerPort servicePort = dockerComposeRule.dockerCompose()
                    .ports(dockerService)
                    .stream()
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("No port defined"));
            RedisConnector redisConnector = isSingleNode()
                    ? new SingleNodeRedisConnector(servicePort.getIp(), servicePort.getExternalPort())
                    : new RedisClusterConnector(servicePort.getIp(), servicePort.getExternalPort(), 12);
            return redisConnector
                    .waitToStartUp(dockerService)
                    .flush()
                    .buildRedisScript(dequeueStrategy);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start service " + dockerService, e);
        }
    }

    protected boolean isSingleNode() {
        return CONTAINERS.get(dockerService);
    }
}
