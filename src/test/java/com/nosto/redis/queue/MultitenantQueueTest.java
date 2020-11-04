/******************************************************************************
 Copyright (c) 2019 Nosto Solutions Ltd All Rights Reserved.
 <p>
 This software is the confidential and proprietary information of
 Nosto Solutions Ltd ("Confidential Information"). You shall not
 disclose such Confidential Information and shall use it only in
 accordance with the terms of the agreement you entered into with
 Nosto Solutions Ltd.
 */
package com.nosto.redis.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class MultitenantQueueTest extends AbstractScriptTest {
    private MultitenantQueue queue;

    @Before
    public void createQueue() {
        queue = new MultitenantQueue("q1", script);
    }

    @Test
    public void delete() {
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
    public void dequeue() {
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

    @SuppressWarnings("Since15")
    @Test
    public void peek() {
        queue.enqueue(new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);
        queue.enqueue(new TenantMessage("t1", "k2", "payload2".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);

        assertEquals(new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8)), queue.peek("t1").orElseThrow());
        assertFalse(queue.peek("t2").isPresent());
    }

    @Test
    public void purge() {
        queue.enqueue(new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);
        queue.enqueue(new TenantMessage("t1", "k2", "payload2".getBytes(StandardCharsets.UTF_8)), Duration.ZERO);

        assertEquals(2, queue.purge("t1"));
    }
}
