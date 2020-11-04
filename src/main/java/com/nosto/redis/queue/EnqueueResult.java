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

/**
 * Describes how messages are enqueued.
 */
public enum EnqueueResult {
    /**
     * The message was successfully enqueued.
     */
    SUCCESS,
    /**
     * The message was successfully enqueued.
     * A previously enqueued message with the same deduplication key was replaced with the newly enqueued message.
     */
    DUPLICATE_OVERWRITTEN
}
