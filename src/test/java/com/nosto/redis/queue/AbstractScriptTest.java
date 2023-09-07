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

import com.nosto.docker.Slf4jLogCollector;
import com.palantir.docker.compose.connection.Ports;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableMap;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;

/**
 * Extend this class to run tests against a single Redis node and a Redis cluster.
 */
@RunWith(Parameterized.class)
public abstract class AbstractScriptTest {

    @ClassRule
    public static final DockerComposeRule DOCKER_RULE = dockerComposeRule();

    // Names of docker services to connect to and a flag to denote if the container is a single node redis instance.
    private static final ImmutableMap<String, Boolean> CONTAINERS = ImmutableMap.<String, Boolean>builder()
            .put("redis6single", true)
            .put("redis6cluster", false)
            .put("redis7single", true)
            .put("redis7cluster", false)
            .build();

    @Parameterized.Parameter
    public String dockerService = "";

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return CONTAINERS.keySet().toArray();
    }

    protected static DockerComposeRule dockerComposeRule() {
        return DockerComposeRule.builder()
                .file("src/test/resources/docker-compose.yml")
                .logCollector(new Slf4jLogCollector())
                .build();
    }

    protected AbstractScript buildScript(DequeueStrategy dequeueStrategy) {
        try {
            Ports ports = DOCKER_RULE.dockerCompose().ports(dockerService);
            DockerPort port = ports
                    .stream()
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("No port defined"));
            RedisConnector redisConnector = isSingleNode()
                    ? new SingleNodeRedisConnector(port.getIp(), port.getExternalPort())
                    : new RedisClusterConnector(port.getIp(), port.getExternalPort(), 12, ports.stream().count());
            return redisConnector
                    .waitToStartUp(dockerService)
                    .flush()
                    .buildRedisScript(dequeueStrategy);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start service " + dockerService, e);
        }
    }

    protected boolean isSingleNode() {
        return BooleanUtils.toBoolean(CONTAINERS.get(dockerService));
    }
}
