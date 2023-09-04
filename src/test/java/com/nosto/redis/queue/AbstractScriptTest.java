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
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableMap;
import com.nosto.redis.RedisClusterConnector;
import com.nosto.redis.SingleNodeRedisConnector;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;

import java.util.stream.IntStream;

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
        DockerComposeRule.Builder builder = DockerComposeRule.builder()
                .file("src/test/resources/docker-compose.yml");
        CONTAINERS.keySet()
                .stream()
                .forEach(service -> builder.waitingForService(service, HealthChecks.toHaveAllPortsOpen()));
        return builder.build();
    }

    protected AbstractScript buildScript(DockerComposeRule dockerComposeRule, DequeueStrategy dequeueStrategy) {
        try {
            DockerPort servicePort = dockerComposeRule.dockerCompose()
                    .ports(dockerService)
                    .stream()
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("No port defined"));
            waitToStartUp(dockerComposeRule.containers().container(dockerService));
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
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start service " + dockerService, e);
        }
    }

    private void waitToStartUp(Container container) {
        int trials = 5;
        if (IntStream.range(0, trials).filter(r -> isHealthy(container)).findFirst().isEmpty()) {
            throw new IllegalStateException("Failed to start service " + dockerService);
        }
    }

    private boolean isHealthy(Container container) {
        try {
            boolean healthy = container.state().isHealthy();
            if (!healthy) {
                Thread.sleep(1000);
            }
            return healthy;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean isSingleNode() {
        return CONTAINERS.get(dockerService);
    }
}
