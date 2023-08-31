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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class MultitenantQueueTest extends AbstractScriptTest {

    @Test
    public void delete() {
        MultitenantQueue queue = buildQueue();

        queue.enqueue(new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);
        queue.enqueue(new TenantMessage("t1", "k2", "payload2".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);

        Map<String, TenantStatistics> qStats = queue.getStatistics()
                .getTenantStatistics();
        assertEquals(1, qStats.size());
        assertEquals(new TenantStatistics("t1", 0, 2), qStats.get("t1"));

        queue.delete("t1", "k1");

        qStats = queue.getStatistics()
                .getTenantStatistics();
        assertEquals(1, qStats.size());
        assertEquals(new TenantStatistics("t1", 0, 1), qStats.get("t1"));

        List<TenantMessage> messages = queue.dequeue(Duration.ofSeconds(2), 10);
        assertEquals(Collections.singletonList(
                new TenantMessage("t1", "k2", "payload2".getBytes(StandardCharsets.UTF_8))), messages);

        qStats = queue.getStatistics()
                .getTenantStatistics();
        assertEquals(1, qStats.size());
        assertEquals(new TenantStatistics("t1", 1, 0), qStats.get("t1"));

        queue.delete("t1", "k2");

        qStats = queue.getStatistics()
                .getTenantStatistics();
        assertEquals(0, qStats.size());
    }

    @Test
    public void dequeueOnePerTenant() {
        MultitenantQueue queue = buildQueue(DequeueStrategy.ONE_PER_TENANT);

        queue.enqueue(new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);
        queue.enqueue(new TenantMessage("t1", "k2", "payload2".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);

        List<TenantMessage> q1Messages = queue.dequeue(Duration.ofSeconds(2), 10);
        assertEquals(Collections.singletonList(
                new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8))), q1Messages);

        Map<String, TenantStatistics> q1Stats = queue.getStatistics()
                .getTenantStatistics();
        assertEquals(1, q1Stats.size());
        assertEquals(new TenantStatistics("t1", 1, 1), q1Stats.get("t1"));
    }

    @Test
    public void dequeueMultiplePerTenant() {
        MultitenantQueue queue = buildQueue(DequeueStrategy.MULTIPLE_PER_TENANT);

        TenantMessage msg1 = new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8));
        TenantMessage msg2 = new TenantMessage("t1", "k2", "payload2".getBytes(StandardCharsets.UTF_8));
        TenantMessage msg3 = new TenantMessage("t1", "k3", "payload3".getBytes(StandardCharsets.UTF_8));
        TenantMessage msg4 = new TenantMessage("t2", "k4", "payload4".getBytes(StandardCharsets.UTF_8));
        TenantMessage msg5 = new TenantMessage("t2", "k5", "payload5".getBytes(StandardCharsets.UTF_8));
        TenantMessage msg6 = new TenantMessage("t2", "k6", "payload6".getBytes(StandardCharsets.UTF_8));

        queue.enqueue(msg1, Duration.ZERO);
        queue.enqueue(msg2, Duration.ZERO);
        queue.enqueue(msg3, Duration.ZERO);
        queue.enqueue(msg4, Duration.ZERO);
        queue.enqueue(msg5, Duration.ZERO);
        queue.enqueue(msg6, Duration.ZERO);

        // Clustered Redis calculates slot (sharding key) based on tenant and dequeue with maximumMessages
        // is called separately for each slot. So in clustered Redis maximumMessages is not maximum number
        // of messages returned by dequeue but maximum number of messages returned per slot and total number
        // of returned messages is maximumMessages multiplied by slot count. To test that more than one
        // message is dequeued per merchant and dequeue does not exceed maximumMessages we set maximumMessages to
        // - 4 in single-node Redis where it is total number of messages for all tenants
        // - 2 in clustered Redis where it is total number of messages for single tenant
        // This way in both cases one message per each tenant is left to queue.
        int maximumMessages = isSingleNode() ? 4 : 2;
        List<TenantMessage> dequeuedMsgs = queue.dequeue(Duration.ofSeconds(2), maximumMessages);
        // Stored expected and dequeued messages to Set for assertion to ignore message order in assertion.
        // This is done because in clustered Redis we cannot guarantee order in which messages of different
        // tenants appear in dequeued message list. Clustered Redis calculates slot (sharding key) based on
        // tenant and dequeue is called separately per slot. Thus order of tenants in single dequeue round
        // is unpredictable but each tenant is dequeued.
        assertEquals(Set.of(msg1, msg2, msg4, msg5), Set.copyOf(dequeuedMsgs));

        Map<String, TenantStatistics> queueStats = queue.getStatistics()
                .getTenantStatistics();

        assertEquals(2, queueStats.size());
        assertEquals(new TenantStatistics("t1", 2, 1), queueStats.get("t1"));
        assertEquals(new TenantStatistics("t2", 2, 1), queueStats.get("t2"));
    }

    @Test
    public void peek() {
        MultitenantQueue queue = buildQueue();

        queue.enqueue(new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);
        queue.enqueue(new TenantMessage("t1", "k2", "payload2".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);

        assertEquals(new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8)), queue.peek("t1").orElse(null));
        assertFalse(queue.peek("t2").isPresent());
    }

    @Test
    public void purge() {
        MultitenantQueue queue = buildQueue();

        queue.enqueue(new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);
        queue.enqueue(new TenantMessage("t1", "k2", "payload2".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);

        assertEquals(2, queue.purge("t1"));
    }

    private MultitenantQueue buildQueue() {
        return buildQueue(DequeueStrategy.ONE_PER_TENANT);
    }

    private MultitenantQueue buildQueue(DequeueStrategy dequeueStrategy) {
        return new MultitenantQueue("q1", buildScript(dequeueStrategy));
    }
}
