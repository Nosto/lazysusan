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
import java.util.Map;

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
    // Names of docker services to connect to and a flag to denote if the container is a single node redis instance.
    private static final Map<String, Boolean> CONTAINERS = ImmutableMap.<String, Boolean>builder()
            .put("redis3single", true)
            .put("redis3cluster", false)
            .put("redis4single", true)
            .put("redis4cluster", false)
            .put("redis5single", true)
            .put("redis5cluster", false)
            .build();

    @ClassRule
    public static final DockerComposeRule DOCKER_RULE = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .build();

    @Parameterized.Parameter
    public String dockerService;

    protected AbstractScript script;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return CONTAINERS.keySet().toArray();
    }

    @Before
    public void setUp() throws IOException, InterruptedException {
        DockerPort servicePort = DOCKER_RULE.dockerCompose()
                .ports(dockerService)
                .stream()
                .findFirst()
                .get();

        if (CONTAINERS.get(dockerService)) {
            SingleNodeRedisConnector singleNodeRedisConnector =
                    new SingleNodeRedisConnector(servicePort.getIp(), servicePort.getExternalPort());
            singleNodeRedisConnector.flush();
            script = new SingleNodeScript(singleNodeRedisConnector.getJedisPool(), 0);
        } else {
            RedisClusterConnector redisClusterConnector =
                    new RedisClusterConnector(servicePort.getIp(), servicePort.getExternalPort());
            redisClusterConnector.flush();
            script = new ClusterScript(redisClusterConnector.getJedisCluster(), 12);
        }
    }
}
