/*******************************************************************************
 * Copyright (c) 2018 Nosto Solutions Ltd All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of
 * Nosto Solutions Ltd ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the agreement you entered into with
 * Nosto Solutions Ltd.
 ******************************************************************************/
package com.nosto.redis.queue;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.nosto.redis.queue.model.Child1Pojo;
import com.nosto.redis.queue.model.Child2Pojo;
import com.nosto.redis.queue.model.ParentPojo;

public class ConnectionManagerTest extends AbstractScriptTest {
    private static final Logger logger = LogManager.getLogger(ConnectionManagerTest.class);

    private static final Duration INVISIBLE_DURATION = Duration.ofMillis(50);
    private static final Duration SHUTDOWN_DURATION = Duration.ofSeconds(2);

    private ConnectionManager connectionManager;

    @After
    public void tearDown() throws Exception {
        if (connectionManager != null && connectionManager.isRunning()) {
            logger.warn("ConnectionManager is still running.");
            connectionManager.shutdownNow();
        }
    }

    /**
     * All messages are successfully sent and received.
     */
    @Test
    public void receivedMessages() throws Exception {
        MessageHandler<Child1Pojo> c1Handler = mock(MessageHandler.class);
        MessageHandler<Child2Pojo> c2Handler = mock(MessageHandler.class);

        configureAndStartConnectionManager(f -> f.withQueueHandler("q1")
                .withMessageHandler(Child1Pojo.class, c1Handler)
                .withMessageHandler(Child2Pojo.class, c2Handler)
                .build());

        MessageSender<Child1Pojo> m1Sender = connectionManager.createSender("q1", m -> "m1_" + m.getPropertyA());

        assertTrue(m1Sender.send("t1", INVISIBLE_DURATION, new Child1Pojo("a1", "b1")));
        assertTrue(m1Sender.send("t1", INVISIBLE_DURATION, new Child1Pojo("a2", "b2")));
        assertTrue(m1Sender.send("t2", INVISIBLE_DURATION, new Child1Pojo("a1", "b1")));

        MessageSender<Child2Pojo> m2Sender = connectionManager.createSender("q1", m -> "m2_" + m.getPropertyA());

        assertTrue(m2Sender.send("t1", INVISIBLE_DURATION, new Child2Pojo("a1", "b1")));
        assertTrue(m2Sender.send("t2", INVISIBLE_DURATION, new Child2Pojo("a1", "b1")));
        assertTrue(m2Sender.send("t2", INVISIBLE_DURATION, new Child2Pojo("a2", "b2")));

        verifyMessagesReceived(Child1Pojo.class, c1Handler, "t1", new Child1Pojo("a1", "b1"), new Child1Pojo("a2", "b2"));
        verifyMessagesReceived(Child1Pojo.class, c1Handler, "t2", new Child1Pojo("a1", "b1"));

        verifyMessagesReceived(Child2Pojo.class, c2Handler, "t1", new Child2Pojo("a1", "b1"));
        verifyMessagesReceived(Child2Pojo.class, c2Handler, "t2", new Child2Pojo("a1", "b1"), new Child2Pojo("a2", "b2"));

        verifyNoMoreInteractions(c1Handler);
        verifyNoMoreInteractions(c2Handler);

        stopConnectionManager();
    }

    /**
     * Message becomes visible again if message handler throws exception while handling.
     */
    @Test
    public void retryOnError() throws Exception {
        MessageHandler<ParentPojo> handler = mock(MessageHandler.class);

        configureAndStartConnectionManager(f -> f.withQueueHandler("q")
                .withMessageHandler(ParentPojo.class, handler)
                .build());

        ParentPojo message = new ParentPojo("a");

        doThrow(new RuntimeException("Ooops"))
                .when(handler).handleMessage(eq("t"), eq(message));

        MessageSender<ParentPojo> messageSender = connectionManager.createSender("q", ParentPojo::getPropertyA);

        assertTrue(messageSender.send("t", INVISIBLE_DURATION, message));

        verifyMessagesReceived(ParentPojo.class, handler, "t", message);

        stopConnectionManager();

        List<AbstractScript.TenantMessage> messages = script.dequeue(Instant.now().plusSeconds(2), "q", 100);
        assertEquals(1, messages.size());
        assertEquals("t", messages.get(0).getTenant());
        assertEquals("a", messages.get(0).getKey());
    }

    /**
     * A handler only handles messages for a specific queue.
     */
    @Test
    public void handlerForQueue() throws Exception {
        MessageHandler<ParentPojo> handler = mock(MessageHandler.class);

        configureAndStartConnectionManager(f -> f.withQueueHandler("q1")
                .withMessageHandler(ParentPojo.class, handler)
                .build());

        MessageSender<ParentPojo> messageSender = connectionManager.createSender("q2", ParentPojo::getPropertyA);
        messageSender.send("t", INVISIBLE_DURATION, new ParentPojo("a"));

        stopConnectionManager();

        // handler is never invoked because the message was sent to q2
        verifyZeroInteractions(handler);
    }

    @Test
    public void startupWithoutHandlers() {
        connectionManager = ConnectionManager.factory()
                .withRedisScript(script)
                .build();

        try {
            connectionManager.start();
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void startupTwice() {
        MessageHandler<ParentPojo> handler = mock(MessageHandler.class);

        configureAndStartConnectionManager(f -> f.withQueueHandler("q1")
                .withMessageHandler(ParentPojo.class, handler)
                .build());

        try {
            connectionManager.start();
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void startupAfterShutdown() throws Exception {
        MessageHandler<ParentPojo> handler = mock(MessageHandler.class);

        configureAndStartConnectionManager(f -> f.withQueueHandler("q1")
                .withMessageHandler(ParentPojo.class, handler)
                .build());

        stopConnectionManager();

        try {
            connectionManager.start();
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
        }
    }

    private void configureAndStartConnectionManager(Function<ConnectionManager.Factory, ConnectionManager.Factory> factoryConfigurator) {
        ConnectionManager.Factory connectionManagerFactory = ConnectionManager.factory()
                .withRedisScript(script);

        connectionManager = factoryConfigurator.apply(connectionManagerFactory)
                .build();

        connectionManager.start();

        assertTrue(connectionManager.isRunning());
    }

    private void stopConnectionManager() throws Exception {
        boolean success = connectionManager.shutdown(SHUTDOWN_DURATION);
        assertTrue(success);
        assertFalse(connectionManager.isRunning());
    }

    private <T> void verifyMessagesReceived(Class<T> c, MessageHandler<T> mockMessageHandler, String expectedTenant, T... expectedMessages) {
        ArgumentCaptor<T> messageCaptor = ArgumentCaptor.forClass(c);

        verify(mockMessageHandler, timeout(SHUTDOWN_DURATION.toMillis()).times(expectedMessages.length))
                .handleMessage(eq(expectedTenant), messageCaptor.capture());

        assertEquals(new HashSet<>(Arrays.asList(expectedMessages)), new HashSet<>(messageCaptor.getAllValues()));
    }
}
