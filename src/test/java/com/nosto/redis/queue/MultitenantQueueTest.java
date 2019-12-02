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
import static org.junit.Assert.assertFalse;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class MultitenantQueueTest extends AbstractScriptTest {
    private MultitenantQueue q1;
    private MultitenantQueue q2;

    @Before
    public void definedQueues() {
        q1 = new MultitenantQueue("q1", script, tenant -> Duration.ZERO);
        q2 = new MultitenantQueue("q2", script, tenant -> Duration.ZERO);
    }

    @Test
    public void multipleQueues() {
        q1.enqueue(new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8)));
        q1.enqueue(new TenantMessage("t1", "k2", "payload2".getBytes(StandardCharsets.UTF_8)));
        q1.enqueue(new TenantMessage("t2", "k1", "payload3".getBytes(StandardCharsets.UTF_8)));

        q2.enqueue(new TenantMessage("t1", "k1", "payload4".getBytes(StandardCharsets.UTF_8)));

        assertEquals(new TenantMessage("t1", "k1", "payload1".getBytes(StandardCharsets.UTF_8)), q1.peek("t1").get());
        assertFalse(q1.peek("t3").isPresent());

        assertEquals(new TenantMessage("t1", "k1", "payload4".getBytes(StandardCharsets.UTF_8)), q2.peek("t1").get());
        assertFalse(q2.peek("t2").isPresent());

        Map<String, TenantStatistics> q1Stats = q1.getStatistics()
                .getTenantStatistics();
        assertEquals(2, q1Stats.size());
        assertEquals(new TenantStatistics("t1", 0, 2), q1Stats.get("t1"));
        assertEquals(new TenantStatistics("t2", 0, 1), q1Stats.get("t2"));

        Map<String, TenantStatistics> q2Stats = q2.getStatistics()
                .getTenantStatistics();
        assertEquals(1, q2Stats.size());
        assertEquals(new TenantStatistics("t1", 0, 1), q2Stats.get("t1"));
    }
}
