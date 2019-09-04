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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.util.Set;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.nosto.redis.queue.model.Child1Pojo;
import com.nosto.redis.queue.model.Child2Pojo;

public class ConnectionManagerTest extends AbstractScriptTest {
    @Test
    public void receivedMessages() {
        MessageHandler<Child1Pojo> c1Handler = (MessageHandler<Child1Pojo>) mock(MessageHandler.class);
        MessageHandler<Child2Pojo> c2Handler = (MessageHandler<Child2Pojo>) mock(MessageHandler.class);

        ConnectionManager connectionManager = new ConnectionManager.Factory()
                .withRedisScript(script)
                .withQueueHandler("queue1", 2)
                    .withMessageHandler(Child1Pojo.class, c1Handler)
                    .withMessageHandler(Child2Pojo.class, c2Handler)
                    .build()
                .build();

        connectionManager.start();

        MessageSender<Child1Pojo> m1Sender = connectionManager.createSender("queue1", m -> "m1_" + m.getPropertyA());

        Duration invisiblePeriod = Duration.ofMillis(50);

        boolean sent = m1Sender.send("t1", invisiblePeriod, new Child1Pojo("a1", "b1"));
        assertTrue(sent);

        sent = m1Sender.send("t1", invisiblePeriod, new Child1Pojo("a2", "b2"));
        assertTrue(sent);

        sent = m1Sender.send("t2", invisiblePeriod, new Child1Pojo("a1", "b1"));
        assertTrue(sent);

        MessageSender<Child2Pojo> m2Sender = connectionManager.createSender("queue1", m -> "m2_" + m.getPropertyA());

        sent = m2Sender.send("t1", invisiblePeriod, new Child2Pojo("a1", "b1"));
        assertTrue(sent);

        sent = m2Sender.send("t2", invisiblePeriod, new Child2Pojo("a1", "b1"));
        assertTrue(sent);

        sent = m2Sender.send("t2", invisiblePeriod, new Child2Pojo("a2", "b2"));
        assertTrue(sent);

        verifyMessagesReceived(Child1Pojo.class, c1Handler, "t1", new Child1Pojo("a1", "b1"), new Child1Pojo("a2", "b2"));
        verifyMessagesReceived(Child1Pojo.class, c1Handler, "t2", new Child1Pojo("a1", "b1"));

        verifyMessagesReceived(Child2Pojo.class, c2Handler, "t1", new Child2Pojo("a1", "b1"));
        verifyMessagesReceived(Child2Pojo.class, c2Handler, "t2", new Child2Pojo("a1", "b1"), new Child2Pojo("a2", "b2"));

        boolean success = connectionManager.shutdown(Duration.ofSeconds(2));
        assertTrue(success);

        verifyNoMoreInteractions(c1Handler);
        verifyNoMoreInteractions(c2Handler);
    }

    private <T> void verifyMessagesReceived(Class<T> c, MessageHandler<T> mockMessageHandler, String expectedTenant, T... expectedMessages) {
        ArgumentCaptor<T> messageCaptor = ArgumentCaptor.forClass(c);

        verify(mockMessageHandler, timeout(1000).times(expectedMessages.length))
                .handleMessage(eq(expectedTenant), messageCaptor.capture());

        assertEquals(Set.of(expectedMessages), Set.copyOf(messageCaptor.getAllValues()));
    }
}
