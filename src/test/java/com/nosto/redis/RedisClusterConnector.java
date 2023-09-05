/*
 *  Copyright (c) 2020 Nosto Solutions Ltd All Rights Reserved.
 *
 *  This software is the confidential and proprietary information of
 *  Nosto Solutions Ltd ("Confidential Information"). You shall not
 *  disclose such Confidential Information and shall use it only in
 *  accordance with the terms of the agreement you entered into with
 *  Nosto Solutions Ltd.
 */
package com.nosto.redis;

import com.nosto.redis.queue.AbstractScript;
import com.nosto.redis.queue.ClusterScript;
import com.nosto.redis.queue.DequeueStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Collection;

@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
public class RedisClusterConnector extends RedisConnector {
    private final JedisCluster jedisCluster;
    private final int numSlots;

    public RedisClusterConnector(String host, int port, int numSlots) {
        jedisCluster = new JedisCluster(new HostAndPort(host, port));
        this.numSlots = numSlots;
    }

    @Override
    public boolean isAlive() {
        Collection<JedisPool> clusterNodes = jedisCluster.getClusterNodes().values();
        long aliveNodeCount = clusterNodes
                .stream()
                .filter(this::ping)
                .count();
        boolean allNodesAlive = clusterNodes.size() == aliveNodeCount;
        System.out.println("=== Redis cluster node count: " + clusterNodes.size());
        if (!allNodesAlive) {
            logger.error("All Redis nodes are not alive yet. Total nodes: " + clusterNodes.size() + ", alive nodes; " + aliveNodeCount);
            System.out.println("=== All Redis nodes are not alive yet. Total nodes: " + clusterNodes.size() + ", alive nodes; " + aliveNodeCount);
        }
        return allNodesAlive;
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
