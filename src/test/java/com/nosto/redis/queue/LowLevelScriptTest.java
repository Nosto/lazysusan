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
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

public class LowLevelScriptTest extends AbstractScriptTest {
    private AbstractScript script;

    @Before
    public void buildScript() {
        script = buildScript(DequeueStrategy.ONE_PER_TENANT);
    }

    @Test
    public void dequeEmpty() {
        assertEquals(Collections.emptyList(),
                script.dequeue(Instant.ofEpochMilli(Long.MAX_VALUE), Duration.ZERO, "q1", 10));
    }

    /**
     * Enqueueing a message for a tenant that is not in the queue should not be visible before a cooldown.
     * This is to prevent a scenario where a message enqueued after dequeueing the las message would exceed
     * the rate limit.
     */
    @Test
    public void enqueueNewTenant() {
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo", "bar".getBytes(StandardCharsets.UTF_8)));
        dequeueAndAssert(Instant.EPOCH, "q1", Duration.ZERO);
        dequeueAndAssert(Instant.EPOCH.plusSeconds(4), "q1", Duration.ZERO);
        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", Duration.ZERO, "bar");
    }

    @Test
    public void unackedMessageBecomesVisible() {
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(1),
                "q1",
                new TenantMessage("t1", "foo", "bar".getBytes(StandardCharsets.UTF_8)));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(1), "q1", Duration.ofSeconds(5), "bar");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(2), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(3), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(4), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(6), "q1", Duration.ofSeconds(10), "bar");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(7), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(8), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(9), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(10), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(11), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(12), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(13), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(14), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(15), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(16), "q1", Duration.ofSeconds(5), "bar");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(17), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(18), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(19), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(20), "q1", Duration.ofSeconds(5));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(21), "q1", Duration.ofSeconds(5), "bar");
    }

    @Test
    public void unackedMessageBecomesVisibleMultipleMessages() {
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(1),
                "q1",
                new TenantMessage("t1", "foo1", "bar1".getBytes(StandardCharsets.UTF_8)));

        script.enqueue(Instant.EPOCH, Duration.ofSeconds(1),
                "q1",
                new TenantMessage("t1", "foo2", "bar2".getBytes(StandardCharsets.UTF_8)));

        dequeueAndAssert(Instant.EPOCH.plusSeconds(1), "q1", Duration.ofSeconds(2), "bar1");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(2), "q1", Duration.ofSeconds(2), "bar2");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(3), "q1", Duration.ofSeconds(2), "bar1");

        script.ack("q1", "t1", "foo1");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(3), "q1", Duration.ofSeconds(2));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(4), "q1", Duration.ofSeconds(2), "bar2");
    }

    @Test
    public void ack() {
        script.enqueue(Instant.EPOCH,
                Duration.ofSeconds(1),
                "q1",
                new TenantMessage("t1", "foo", "bar".getBytes(StandardCharsets.UTF_8)));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(1), "q1", Duration.ZERO, "bar");
        // Acking message should return true indicating that message did exist in queue.
        assertTrue(script.ack("q1", "t1", "foo"));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(2), "q1", Duration.ZERO);
        // Acking same message again should return false indicating that message did not exist in queue anymore.
        assertFalse(script.ack("q1", "t1", "foo"));
    }

    @Test
    public void enqueueExistingTenant() {
        script.enqueue(Instant.EPOCH,
                Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo1", "bar1".getBytes(StandardCharsets.UTF_8)));
        script.enqueue(Instant.EPOCH.plusSeconds(2),
                Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo2", "bar2".getBytes(StandardCharsets.UTF_8)));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(4), "q1", Duration.ZERO);
        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", Duration.ZERO, "bar1");
        script.ack("q1", "t1", "foo1");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(9), "q1", Duration.ZERO);
        dequeueAndAssert(Instant.EPOCH.plusSeconds(10), "q1", Duration.ZERO, "bar2");
    }

    @Test
    public void dequeueMultiple() {
        script.enqueue(Instant.EPOCH,
                Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo", "bar".getBytes(StandardCharsets.UTF_8)));
        script.enqueue(Instant.EPOCH,
                Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t2", "foo1", "bar1".getBytes(StandardCharsets.UTF_8)));
        script.enqueue(Instant.EPOCH,
                Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t2", "foo2", "bar2".getBytes(StandardCharsets.UTF_8)));
        script.enqueue(Instant.EPOCH,
                Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t3", "foo", "bar".getBytes(StandardCharsets.UTF_8)));

        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", Duration.ZERO, "bar", "bar1", "bar");
    }

    @Test
    public void deduplication() {
        TenantMessage msg1 =
                new TenantMessage("t1", "foo", "bar".getBytes(StandardCharsets.UTF_8));
        TenantMessage msg2 =
                new TenantMessage("t2", "foo", "bar".getBytes(StandardCharsets.UTF_8));

        assertEquals(EnqueueResult.SUCCESS, script.enqueue(Instant.EPOCH, Duration.ofSeconds(5), "q1", msg1));
        assertEquals(EnqueueResult.DUPLICATE_OVERWRITTEN,
                script.enqueue(Instant.EPOCH, Duration.ofSeconds(5), "q1", msg1));

        assertEquals(EnqueueResult.SUCCESS, script.enqueue(Instant.EPOCH, Duration.ofSeconds(5), "q1", msg2));
        assertEquals(EnqueueResult.DUPLICATE_OVERWRITTEN,
                script.enqueue(Instant.EPOCH, Duration.ofSeconds(5), "q1", msg2));
    }

    /**
     * If two messages have different payload but same de-duplication key,
     * the latest one takes precedence.
     */
    @Test
    public void latestTakesPrecedence() {
        TenantMessage msg1 =
                new TenantMessage("t1", "foo", "bar1".getBytes(StandardCharsets.UTF_8));
        TenantMessage msg2 =
                new TenantMessage("t1", "foo", "bar2".getBytes(StandardCharsets.UTF_8));

        assertEquals(EnqueueResult.SUCCESS, script.enqueue(Instant.EPOCH, Duration.ofSeconds(5), "q1", msg1));
        assertEquals(EnqueueResult.DUPLICATE_OVERWRITTEN,
                script.enqueue(Instant.EPOCH, Duration.ofSeconds(5), "q1", msg2));

        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", Duration.ZERO, "bar2");
    }

    /**
     * An enqueued message should be deduplicated if an invisible message has the same key.
     */
    @Test
    public void deduplicateWithInvisibleMessage() {
        TenantMessage msg1 =
                new TenantMessage("t1", "foo", "bar1".getBytes(StandardCharsets.UTF_8));
        TenantMessage msg2 =
                new TenantMessage("t1", "foo", "bar2".getBytes(StandardCharsets.UTF_8));

        assertEquals(EnqueueResult.SUCCESS, script.enqueue(Instant.EPOCH, Duration.ofSeconds(1), "q1", msg1));

        dequeueAndAssert(Instant.EPOCH.plusSeconds(2), "q1", Duration.ofSeconds(2), "bar1");

        assertEquals(EnqueueResult.DUPLICATE_INVISIBLE, script.enqueue(Instant.EPOCH, Duration.ofSeconds(1), "q1", msg2));

        dequeueAndAssert(Instant.EPOCH.plusSeconds(10), "q1", Duration.ZERO, "bar1");
    }

    @Test
    public void enqueueJson() {
        String payload = "{\"key\":\"bar\"}";
        script.enqueue(Instant.EPOCH,
                Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo", payload.getBytes(StandardCharsets.UTF_8)));
        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", Duration.ZERO, payload);
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
        List<TenantMessage> messages = script.dequeue(later, Duration.ZERO, "q1", 100);
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

    @Test
    public void peekEmpty() {
        Optional<TenantMessage> message = script.peek(Instant.ofEpochMilli(Long.MAX_VALUE), "q1", "t1");
        assertFalse(message.isPresent());
    }

    @Test
    public void peek() {
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo1", "bar1".getBytes(StandardCharsets.UTF_8)));
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo2", "bar2".getBytes(StandardCharsets.UTF_8)));

        Optional<TenantMessage> message = script.peek(Instant.EPOCH, "q1", "t1");
        assertTrue(message.isPresent());
        assertEquals("t1", message.get().getTenant());
        assertEquals("foo1", message.get().getKey());
        assertEquals("bar1", new String(message.get().getPayload(), StandardCharsets.UTF_8));

        message = script.peek(Instant.EPOCH, "q1", "t2");
        assertFalse(message.isPresent());

        message = script.peek(Instant.EPOCH, "q2", "t1");
        assertFalse(message.isPresent());
    }

    @Test
    public void purge() {
        // Enqueue messages for t1 to q1 & q2
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo1", "bar1".getBytes(StandardCharsets.UTF_8)));
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo2", "bar2".getBytes(StandardCharsets.UTF_8)));
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q2",
                new TenantMessage("t1", "foo1", "bar3".getBytes(StandardCharsets.UTF_8)));

        // Enqueue messages for t2 to q1 & q2
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t2", "foo1", "bar4".getBytes(StandardCharsets.UTF_8)));
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t2", "foo2", "bar5".getBytes(StandardCharsets.UTF_8)));
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q2",
                new TenantMessage("t2", "foo1", "bar6".getBytes(StandardCharsets.UTF_8)));

        // Dequeue from q1 and q2 verify visible and invisible messages are dequeued.
        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q1", Duration.ZERO, "bar1", "bar4");
        dequeueAndAssert(Instant.EPOCH.plusSeconds(5), "q2", Duration.ZERO, "bar3", "bar6");

        // Verify statistics before purge
        QueueStatistics statistics = script.getQueueStatistics("q1");
        assertEquals(2, statistics.getTenantStatistics().size());
        assertEquals(new TenantStatistics("t1", 1, 1), statistics.getTenantStatistics().get("t1"));
        assertEquals(new TenantStatistics("t2", 1, 1), statistics.getTenantStatistics().get("t2"));

        statistics = script.getQueueStatistics("q2");
        assertEquals(2, statistics.getTenantStatistics().size());
        assertEquals(new TenantStatistics("t1", 1, 0), statistics.getTenantStatistics().get("t1"));
        assertEquals(new TenantStatistics("t2", 1, 0), statistics.getTenantStatistics().get("t2"));

        // 1 visible and 1 invisible message purged for t1 in q1
        assertEquals(2, script.purge("q1", "t1"));

        // Verify statistics after purge
        statistics = script.getQueueStatistics("q1");
        assertEquals(1, statistics.getTenantStatistics().size());
        assertEquals(new TenantStatistics("t2", 1, 1), statistics.getTenantStatistics().get("t2"));

        statistics = script.getQueueStatistics("q2");
        assertEquals(2, statistics.getTenantStatistics().size());
        assertEquals(new TenantStatistics("t1", 1, 0), statistics.getTenantStatistics().get("t1"));
        assertEquals(new TenantStatistics("t2", 1, 0), statistics.getTenantStatistics().get("t2"));

        // Enqueue messages again to verify enqueue, dequeue and statistics work as normal after purge
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo1", "bar1".getBytes(StandardCharsets.UTF_8)));
        script.enqueue(Instant.EPOCH, Duration.ofSeconds(5),
                "q1",
                new TenantMessage("t1", "foo2", "bar2".getBytes(StandardCharsets.UTF_8)));

        dequeueAndAssert(Instant.EPOCH.plusSeconds(10), "q1", Duration.ZERO, "bar1", "bar4");

        statistics = script.getQueueStatistics("q1");
        assertEquals(2, statistics.getTenantStatistics().size());
        assertEquals(new TenantStatistics("t1", 1, 1), statistics.getTenantStatistics().get("t1"));
        assertEquals(new TenantStatistics("t2", 1, 1), statistics.getTenantStatistics().get("t2"));
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
            script.enqueue(now, duration, queue, new TenantMessage(tenant,
                    Integer.toString(key++), "bar".getBytes(StandardCharsets.UTF_8)));
        }

        for (int i = 0; i < laterMessages; i++) {
            script.enqueue(later, duration, queue, new TenantMessage(tenant,
                    Integer.toString(key++), "bar".getBytes(StandardCharsets.UTF_8)));
        }
    }

    private void dequeueAndAssert(Instant now, String queue, Duration invisiblePeriod, String... expected) {
        List<String> expectedList = Arrays.asList(expected);
        Collections.sort(expectedList);
        assertEquals(expectedList,
                script.dequeue(now, invisiblePeriod, queue, 10)
                        .stream()
                        .map(TenantMessage::getPayload)
                        .map(String::new)
                        .sorted()
                        .collect(Collectors.toList()));
    }
}
