/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.util.concurrent;

import java.util.concurrent.CountDownLatch;

/**
 * Event bus abstraction for internal task queues.
 * <p>
 * Supports both Disruptor (object reuse) and MPSC (no reuse) implementations
 * to address cross-generation reference issues with generational GC.
 *
 * @param <T> the event type
 * @author dennis
 * @see <a href="https://github.com/sofastack/sofa-jraft/issues/1231">Issue #1231</a>
 */
public interface EventBus<T> {

    /**
     * Publish an event to the bus.
     * <p>
     * This method may block if the bus is full (blocking mode).
     *
     * @param event the event to publish
     * @return true if published successfully, false if shutdown
     */
    boolean publish(T event);

    /**
     * Try to publish an event without blocking.
     *
     * @param event the event to publish
     * @return true if published, false if no capacity or shutdown
     */
    boolean tryPublish(T event);

    /**
     * Check if there is available capacity for publishing.
     *
     * @param requiredCapacity required number of slots
     * @return true if capacity is available
     */
    boolean hasAvailableCapacity(int requiredCapacity);

    /**
     * Get the buffer/queue size (maximum capacity).
     *
     * @return buffer size
     */
    int getBufferSize();

    /**
     * Get the current number of pending events in the queue.
     *
     * @return current queue size
     */
    int getPendingEventCount();

    /**
     * Shutdown the event bus gracefully.
     * <p>
     * The latch will be counted down when shutdown is complete
     * and all pending events have been processed.
     *
     * @param latch countdown when shutdown complete
     */
    void shutdown(CountDownLatch latch);

    /**
     * Check if the bus is shutdown.
     *
     * @return true if shutdown
     */
    boolean isShutdown();
}
