/*
 *  Copyright (c) 2023 Nosto Solutions Ltd All Rights Reserved.
 *
 *  This software is the confidential and proprietary information of
 *  Nosto Solutions Ltd ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the agreement you entered into with
 *  Nosto Solutions Ltd.
 */
package com.nosto.redis.queue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;

@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
public class RedisClusterConnector extends RedisConnector {
    private final JedisCluster jedisCluster;
    private final int numSlots;
    private final long nodeCount;

    public RedisClusterConnector(String host, int port, int numSlots, long nodeCount) {
        jedisCluster = new JedisCluster(new HostAndPort(host, port));
        this.numSlots = numSlots;
        this.nodeCount = nodeCount;
    }

    @Override
    public boolean isAlive() {
        long aliveNodeCount = jedisCluster.getClusterNodes()
                .values()
                .stream()
                .filter(this::isAlive)
                .count();
        boolean allNodesAlive = nodeCount == aliveNodeCount;
        if (!allNodesAlive) {
            logger.info("All Redis nodes are not alive yet. Total nodes: " + nodeCount + ", alive nodes: " + aliveNodeCount);
        }
        return allNodesAlive;
    }

    private boolean isAlive(JedisPool jedisPool) {
        try (Jedis jedis = jedisPool.getResource()) {
            String clusterState = jedis.clusterInfo();
            boolean alive = clusterState.startsWith("cluster_state:ok");
            if (!alive) {
                logger.info("Redis not alive. Response: " + clusterState);
            }
            return alive;
        } catch (Exception e) {
            logger.error("Redis ping failed.", e);
            return false;
        }
    }

    @Override
    public RedisConnector flush() {
        jedisCluster.getClusterNodes().forEach((key, jedisPool) -> {
            try (Jedis j = jedisPool.getResource()) {
                j.flushDB();
            } catch (JedisDataException e) {
                // redis.clients.jedis.exceptions.JedisDataException: READONLY You can't write against a read only slave
            }
        });
        return this;
    }

    @Override
    public AbstractScript buildRedisScript(DequeueStrategy dequeueStrategy) {
        return new ClusterScript(jedisCluster, numSlots, dequeueStrategy);
    }
}
