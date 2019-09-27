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

/**
 * Handle dequeued messages.
 * @param <T>
 */
public interface MessageHandler<T> {
    /**
     * Handle a dequeued message.
     *
     * @param tenant The message's tenant.
     * @param message The deserialized message payload.
     * @param completionNotifier Call {@link CompletionNotifier#completed()} after message handling has completed
     *                           successfully.
     */
    void handleMessage(String tenant, T message, CompletionNotifier completionNotifier);

    /**
     * @return The {@link Class} that this implementation handles.
     */
    Class<T> getMessageClass();

    /**
     * Allows the {@link MessageHandler} to notify that the message handling has completed successfully.
     */
    interface CompletionNotifier {
        /**
         * Message has been handled successfully and can be safely deleted from the queue.
         */
        void completed();
    }
}
