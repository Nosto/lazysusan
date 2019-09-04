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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.nosto.redis.queue.model.Child1Pojo;
import com.nosto.redis.queue.model.Child2Pojo;
import com.nosto.redis.queue.model.ParentPojo;

public class ConnectionManagerTest extends AbstractScriptTest {
    /**
     * All messages are successfully sent and received.
     */
    @Test
    public void receivedMessages() {
        MessageHandler<Child1Pojo> c1Handler = mock(MessageHandler.class);
        MessageHandler<Child2Pojo> c2Handler = mock(MessageHandler.class);

        ConnectionManager connectionManager = new ConnectionManager.Factory()
                .withRedisScript(script)
                .withQueueHandler("q1", 2)
                    .withMessageHandler(Child1Pojo.class, c1Handler)
                    .withMessageHandler(Child2Pojo.class, c2Handler)
                    .build()
                .build();

        connectionManager.start();

        Duration invisiblePeriod = Duration.ofMillis(50);

        MessageSender<Child1Pojo> m1Sender = connectionManager.createSender("q1", m -> "m1_" + m.getPropertyA());

        assertTrue(m1Sender.send("t1", invisiblePeriod, new Child1Pojo("a1", "b1")));
        assertTrue(m1Sender.send("t1", invisiblePeriod, new Child1Pojo("a2", "b2")));
        assertTrue(m1Sender.send("t2", invisiblePeriod, new Child1Pojo("a1", "b1")));

        MessageSender<Child2Pojo> m2Sender = connectionManager.createSender("q1", m -> "m2_" + m.getPropertyA());

        assertTrue(m2Sender.send("t1", invisiblePeriod, new Child2Pojo("a1", "b1")));
        assertTrue(m2Sender.send("t2", invisiblePeriod, new Child2Pojo("a1", "b1")));
        assertTrue(m2Sender.send("t2", invisiblePeriod, new Child2Pojo("a2", "b2")));

        verifyMessagesReceived(Child1Pojo.class, c1Handler, "t1", new Child1Pojo("a1", "b1"), new Child1Pojo("a2", "b2"));
        verifyMessagesReceived(Child1Pojo.class, c1Handler, "t2", new Child1Pojo("a1", "b1"));

        verifyMessagesReceived(Child2Pojo.class, c2Handler, "t1", new Child2Pojo("a1", "b1"));
        verifyMessagesReceived(Child2Pojo.class, c2Handler, "t2", new Child2Pojo("a1", "b1"), new Child2Pojo("a2", "b2"));

        boolean success = connectionManager.shutdown(Duration.ofSeconds(2));
        assertTrue(success);

        verifyNoMoreInteractions(c1Handler);
        verifyNoMoreInteractions(c2Handler);
    }

    /**
     * Message becomes visible again if message handler throws exception while handling.
     */
    @Test
    public void retryOnError() {
        MessageHandler<ParentPojo> handler = mock(MessageHandler.class);

        ConnectionManager connectionManager = new ConnectionManager.Factory()
                .withRedisScript(script)
                .withQueueHandler("q", 1)
                .withMessageHandler(ParentPojo.class, handler)
                .build()
                .build();

        connectionManager.start();

        ParentPojo message = new ParentPojo("a");

        doThrow(new RuntimeException("Ooops"))
                .when(handler).handleMessage(eq("t"), eq(message));

        MessageSender<ParentPojo> messageSender = connectionManager.createSender("q", ParentPojo::getPropertyA);
        
        assertTrue(messageSender.send("t", Duration.ofMillis(50), message));
        
        verifyMessagesReceived(ParentPojo.class, handler, "t", message);

        boolean success = connectionManager.shutdown(Duration.ofSeconds(2));
        assertTrue(success);

        verifyNoMoreInteractions(handler);

        List<AbstractScript.TenantMessage> messages = script.dequeue(Instant.now().plusSeconds(2), "q", 100);
        assertEquals(1, messages.size());
        assertEquals("t", messages.get(0).getTenant());
        assertEquals("a", messages.get(0).getKey());
    }

    private <T> void verifyMessagesReceived(Class<T> c, MessageHandler<T> mockMessageHandler, String expectedTenant, T... expectedMessages) {
        ArgumentCaptor<T> messageCaptor = ArgumentCaptor.forClass(c);

        verify(mockMessageHandler, timeout(1000).times(expectedMessages.length))
                .handleMessage(eq(expectedTenant), messageCaptor.capture());

        assertEquals(Set.of(expectedMessages), Set.copyOf(messageCaptor.getAllValues()));
    }
}
