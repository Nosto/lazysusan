# Lazy Susan
Lazy Susan is a Redis-based FIFO multi-tenant queue.

## Connecting to Redis
Lazy Susan works with either a single Redis node or a Redis cluster. Simply use the corresponding `com.nosto.redis.queue.MultitenantQueue` constructor, depending on which Redis configuration you wish to use.

## Dequeue Interval
Implement the `com.nosto.redis.queue.DequeueIntervalProvider` interface to specify the rate at which messages are dequeued.

## Enqeueing Messages
Call `com.nosto.redis.queue.MultitenantQueue.enqueue` to enqueue a message.

### De-duplication 
If a message with the same key already exists in the queue, the newly enqueued message overwrites the previously enqueued message and `com.nosto.redis.queue.EnqueueResult.DUPLICATE_OVERWRITTEN` is returned.

## De-queueing Messages
Call `com.nosto.redis.queue.MultitenantQueue.dequeue` to deqeue messages. 

### Message Visibility
The `messageInvisibilityPeriod` parameter is used to mark each dequeued message as invisible. Invisible messages cannot be dequeued until `messageInvisibilityPeriod` ellapses. Invisible messages can be deleted by calling `com.nosto.redis.queue.MultitenantQueue.delete`.

## Monitoring Queues
Call `com.nosto.redis.queue.MultitenantQueue.peek` to fetch a message from a queue without marking it as invisible.

The number messages, broken down by tenant, can be queried for a queue by calling `com.nosto.redis.queue.MultitenantQueue.getStatistics`.

A queue can be purged by calling `com.nosto.redis.queue.MultitenantQueue.purge`.