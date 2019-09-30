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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import com.nosto.redis.queue.AbstractScript.TenantMessage;

public class LowLevelScriptTest extends AbstractScriptTest {
    @Test
    public void dequeEmpty() {
        assertEquals(Collections.emptyList(), script.dequeue(Instant.ofEpochMilli(Long.MAX_VALUE), "q1", 10));
    }

    /**
     * Enqueueing a message for a tenant that is not in the queue should not be visible before a cooldown.
     * This is to prevent a scenario where a message enqueued after dequeueing the las message would exceed
     * the rate limit.
     */
    @Test
    public void enqueueNewTenant() {
        TenantMessage msg =
                new TenantMessage("t1", "foo", "bar".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(5));
        script.enqueue(Instant.EPOCH, "q1", msg);
        dequeueAndAssert(Instant.EPOCH, "q1");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(4), "q1");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", msg);
    }

    @Test
    public void unackedMessageBecomesVisible() {
        TenantMessage message =
                new TenantMessage("t1", "foo", "bar".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(1));
        script.enqueue(Instant.EPOCH, "q1", message);
        dequeueAndAssert(Instant.EPOCH.plusSeconds(1), "q1", message);
        dequeueAndAssert(Instant.EPOCH.plusSeconds(1), "q1");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(2), "q1", message);
    }

    @Test
    public void ack() {
        TenantMessage message =
                new TenantMessage("t1", "foo", "bar".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(1));
        script.enqueue(Instant.EPOCH, "q1", message);
        dequeueAndAssert(Instant.EPOCH.plusSeconds(1), "q1", message);
        script.ack("q1", "t1", "foo");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(2), "q1");
    }

    @Test
    public void enqueueExistingTenant() {
        TenantMessage msg1 =
                new TenantMessage("t1", "foo1", "bar1".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(5));
        script.enqueue(Instant.EPOCH, "q1", msg1);

        TenantMessage msg2 =
                new TenantMessage("t1", "foo2", "bar2".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(5));
        script.enqueue(Instant.EPOCH.plusSeconds(2), "q1", msg2);

        dequeueAndAssert(Instant.EPOCH.plusSeconds(4), "q1");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", msg1);

        script.ack("q1", "t1", "foo1");

        dequeueAndAssert(Instant.EPOCH.plusSeconds(9), "q1");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(10), "q1", msg2);
    }

    @Test
    public void dequeueMultiple() {
        TenantMessage msg1 =
                new TenantMessage("t1", "foo", "bar".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(5));
        script.enqueue(Instant.EPOCH, "q1", msg1);

        TenantMessage msg2 =
                new TenantMessage("t2", "foo1", "bar1".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(6));
        script.enqueue(Instant.EPOCH, "q1", msg2);

        TenantMessage msg3 =
                new TenantMessage("t2", "foo2", "bar2".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(7));
        script.enqueue(Instant.EPOCH, "q1", msg3);

        TenantMessage msg4 =
                new TenantMessage("t3", "foo", "bar".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(8));
        script.enqueue(Instant.EPOCH, "q1", msg4);

        TenantMessage t2Msg =
                new TenantMessage("t2", "foo1", "bar1".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(7));

        dequeueAndAssert(Instant.EPOCH.plusSeconds(9), "q1", msg1, t2Msg, msg4);
    }

    @Test
    public void deduplication() {
        TenantMessage msg1 =
                new TenantMessage("t1", "foo", "bar".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(4));
        TenantMessage msg2 =
                new TenantMessage("t2", "foo", "bar".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(5));

        assertEquals(EnqueueResult.SUCCESS, script.enqueue(Instant.EPOCH, "q1", msg1));
        assertEquals(EnqueueResult.DUPLICATE_OVERWRITTEN,
                script.enqueue(Instant.EPOCH, "q1", msg1));

        assertEquals(EnqueueResult.SUCCESS, script.enqueue(Instant.EPOCH, "q1", msg2));
        assertEquals(EnqueueResult.DUPLICATE_OVERWRITTEN,
                script.enqueue(Instant.EPOCH, "q1", msg2));
    }

    /**
     * If two messages have different payload but same de-duplication key,
     * the latest one takes precedence.
     */
    @Test
    public void latestTakesPrecedence() {
        TenantMessage msg1 =
                new TenantMessage("t1", "foo", "bar1".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(4));
        TenantMessage msg2 =
                new TenantMessage("t1", "foo", "bar2".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(5));

        assertEquals(EnqueueResult.SUCCESS, script.enqueue(Instant.EPOCH, "q1", msg1));
        assertEquals(EnqueueResult.DUPLICATE_OVERWRITTEN,
                script.enqueue(Instant.EPOCH, "q1", msg2));

        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", msg2);
    }

    @Test
    public void enqueueJson() {
        String payload = "{\"key\":\"bar\"}";
        TenantMessage msg =
                new TenantMessage("t1", "foo", payload.getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(5));
        script.enqueue(Instant.EPOCH, "q1", msg);
        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", msg);
    }

    @Test
    public void getQueueStatistics() {
        QueueStatistics stats = script.getQueueStatistics("q1");
        assertTrue(stats.getTenantStatistics().isEmpty());

        stats = script.getQueueStatistics("q2");
        assertTrue(stats.getTenantStatistics().isEmpty());

        Instant now = Instant.now();
        Instant later = now.plusSeconds(60);

        enqueueMessages("q1", "t1", now, 2, later, 3);
        enqueueMessages("q1", "t2", now, 1, later, 1);

        // Different statistics for each tenant
        stats = script.getQueueStatistics("q1");
        assertEquals(2, stats.getTenantStatistics().size());
        assertEquals(new TenantStatistics("t1", 0, 5), stats.getTenantStatistics().get("t1"));
        assertEquals(new TenantStatistics("t2", 0, 2), stats.getTenantStatistics().get("t2"));

        stats = script.getQueueStatistics("q2");
        assertTrue(stats.getTenantStatistics().isEmpty());

        // Dequeueing messages makes 1 message invisible per tenant
        List<TenantMessage> messages = script.dequeue(later, "q1", 100);
        assertEquals(2, messages.size());

        stats = script.getQueueStatistics("q1");
        assertEquals(2, stats.getTenantStatistics().size());
        assertEquals(new TenantStatistics("t1", 1, 4), stats.getTenantStatistics().get("t1"));
        assertEquals(new TenantStatistics("t2", 1, 1), stats.getTenantStatistics().get("t2"));

        stats = script.getQueueStatistics("q2");
        assertTrue(stats.getTenantStatistics().isEmpty());

        // Enqueue messages to another queue
        enqueueMessages("q2", "t1", now, 3, later, 4);
        enqueueMessages("q2", "t2", now, 3, later, 2);

        // q1 stats remain the same
        stats = script.getQueueStatistics("q1");
        assertEquals(2, stats.getTenantStatistics().size());
        assertEquals(new TenantStatistics("t1", 1, 4), stats.getTenantStatistics().get("t1"));
        assertEquals(new TenantStatistics("t2", 1, 1), stats.getTenantStatistics().get("t2"));

        // q2 stats now returned
        stats = script.getQueueStatistics("q2");
        assertEquals(2, stats.getTenantStatistics().size());
        assertEquals(new TenantStatistics("t1", 0, 7), stats.getTenantStatistics().get("t1"));
        assertEquals(new TenantStatistics("t2", 0, 5), stats.getTenantStatistics().get("t2"));
    }

    private void enqueueMessages(String queue,
                                 String tenant,
                                 Instant now,
                                 int nowMessages,
                                 Instant later,
                                 int laterMessages) {
        Duration duration = Duration.ofMillis(1);
        int key = 0;

        for (int i = 0; i < nowMessages; i++) {
            script.enqueue(now, queue, new TenantMessage(tenant,
                    Integer.toString(key++), "bar".getBytes(StandardCharsets.UTF_8), duration));
        }

        for (int i = 0; i < laterMessages; i++) {
            script.enqueue(later, queue, new TenantMessage(tenant,
                    Integer.toString(key++), "bar".getBytes(StandardCharsets.UTF_8), duration));
        }
    }

    private void dequeueAndAssert(Instant now, String queue, TenantMessage... expected) {
        List<TenantMessage> expectedList = Arrays.asList(expected);
        expectedList.sort(Comparator.comparing(TenantMessage::getInvisiblePeriod));

        List<TenantMessage> actualList = script.dequeue(now, queue, 10);
        actualList.sort(Comparator.comparing(TenantMessage::getInvisiblePeriod));

        assertEquals(expectedList, actualList);
    }
}
