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

import static com.nosto.redis.queue.ConnectionManagerTest.createMockMessageHandler;
import static com.nosto.redis.queue.ConnectionManagerTest.stopConnectionManager;
import static com.nosto.redis.queue.ConnectionManagerTest.verifyMessagesReceived;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import java.time.Duration;
import java.util.function.Function;

import org.junit.Test;

import com.nosto.redis.queue.ConnectionManager.Factory;
import com.nosto.redis.queue.model.Child1Pojo;

public class StubbedConnectionManagerTest {
    private static final Duration INVISIBLE_DURATION = Duration.ofMillis(0);

    private ConnectionManager connectionManager;

    @Test
    public void receivedMessages() throws Exception {
        MessageHandler<Child1Pojo> c1Handler = createMockMessageHandler(Child1Pojo.class);

        configureAndStartConnectionManager(f -> f
                .withQueueHandler("q1", INVISIBLE_DURATION, 1, c1Handler)
                .withQueueHandler("q2", INVISIBLE_DURATION, 1, c1Handler)
                .withClientStub());

        MessageSender<Child1Pojo> q1Sender = connectionManager.createSender("q1", m -> "m1_" + m.getPropertyA());
        MessageSender<Child1Pojo> q2Sender = connectionManager.createSender("q2", m -> "m1_" + m.getPropertyA());

        assertEquals(EnqueueResult.SUCCESS, q1Sender.send("t1", INVISIBLE_DURATION, new Child1Pojo("a1", "b1")));
        assertEquals(EnqueueResult.SUCCESS, q2Sender.send("t1", INVISIBLE_DURATION, new Child1Pojo("a1", "b1")));

        verifyMessagesReceived(Duration.ZERO, Child1Pojo.class, c1Handler, "t1",
                new Child1Pojo("a1", "b1"),
                new Child1Pojo("a1", "b1"));

        stopConnectionManager(connectionManager);
    }

    private void configureAndStartConnectionManager(Function<Factory,
            Factory> factoryConfigurator) {
        ConnectionManager.Factory connectionManagerFactory = ConnectionManager.factory()
                .withClientStub()
                .withPolymorphicJacksonMessageConverter();

        connectionManager = factoryConfigurator.apply(connectionManagerFactory)
                .build();

        assertFalse(connectionManager.isRunning());

        connectionManager.start();

        assertTrue(connectionManager.isRunning());
    }
}
