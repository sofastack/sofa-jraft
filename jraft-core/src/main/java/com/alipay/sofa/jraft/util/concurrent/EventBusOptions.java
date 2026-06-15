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

import java.util.concurrent.ThreadFactory;

import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Configuration options for {@link EventBus}.
 *
 * @author dennis
 */
public class EventBusOptions {

    private static final int DEFAULT_BUFFER_SIZE    = 16384;
    private static final int DEFAULT_MAX_BATCH_SIZE = 32;
    private static final int DEFAULT_WAIT_TIMEOUT   = 1000;

    private EventBusMode     mode                   = EventBusMode.DISRUPTOR;
    private int              bufferSize             = DEFAULT_BUFFER_SIZE;
    private int              maxBatchSize           = DEFAULT_MAX_BATCH_SIZE;
    private String           name                   = "EventBus";
    private ThreadFactory    threadFactory;
    private WaitStrategyType waitStrategy           = WaitStrategyType.BLOCKING;
    private int              waitTimeoutMs          = DEFAULT_WAIT_TIMEOUT;

    public EventBusOptions() {
        this.threadFactory = new NamedThreadFactory(this.name + "-", true);
    }

    /**
     * Get the event bus mode.
     */
    public EventBusMode getMode() {
        return this.mode;
    }

    /**
     * Set the event bus mode.
     *
     * @param mode event bus mode
     * @return this options instance for chaining
     */
    public EventBusOptions setMode(final EventBusMode mode) {
        this.mode = Requires.requireNonNull(mode, "mode");
        return this;
    }

    /**
     * Get the buffer/queue size.
     */
    public int getBufferSize() {
        return this.bufferSize;
    }

    /**
     * Set the buffer/queue size.
     * <p>
     * For Disruptor mode: this is the RingBuffer size (must be power of 2).
     * For MPSC mode: this is the maximum queue capacity.
     *
     * @param bufferSize buffer size
     * @return this options instance for chaining
     */
    public EventBusOptions setBufferSize(final int bufferSize) {
        Requires.requireTrue(bufferSize > 0, "bufferSize must be positive");
        this.bufferSize = bufferSize;
        return this;
    }

    /**
     * Get the maximum batch size for MPSC mode.
     */
    public int getMaxBatchSize() {
        return this.maxBatchSize;
    }

    /**
     * Set the maximum batch size.
     * <p>
     * This controls when {@code endOfBatch} becomes true in the handler.
     * Only applicable for MPSC mode; Disruptor determines batch boundaries internally.
     *
     * @param maxBatchSize maximum events to process before forcing endOfBatch=true
     * @return this options instance for chaining
     */
    public EventBusOptions setMaxBatchSize(final int maxBatchSize) {
        Requires.requireTrue(maxBatchSize > 0, "maxBatchSize must be positive");
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    /**
     * Get the event bus name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Set the event bus name.
     * <p>
     * Used for thread naming and logging.
     *
     * @param name event bus name
     * @return this options instance for chaining
     */
    public EventBusOptions setName(final String name) {
        this.name = Requires.requireNonNull(name, "name");
        return this;
    }

    /**
     * Get the thread factory.
     */
    public ThreadFactory getThreadFactory() {
        return this.threadFactory;
    }

    /**
     * Set the thread factory for consumer thread.
     *
     * @param threadFactory thread factory
     * @return this options instance for chaining
     */
    public EventBusOptions setThreadFactory(final ThreadFactory threadFactory) {
        this.threadFactory = Requires.requireNonNull(threadFactory, "threadFactory");
        return this;
    }

    /**
     * Get the wait strategy type.
     */
    public WaitStrategyType getWaitStrategy() {
        return this.waitStrategy;
    }

    /**
     * Set the wait strategy type for consumer thread.
     *
     * @param waitStrategy wait strategy type
     * @return this options instance for chaining
     */
    public EventBusOptions setWaitStrategy(final WaitStrategyType waitStrategy) {
        this.waitStrategy = Requires.requireNonNull(waitStrategy, "waitStrategy");
        return this;
    }

    /**
     * Get the wait timeout in milliseconds.
     */
    public int getWaitTimeoutMs() {
        return this.waitTimeoutMs;
    }

    /**
     * Set the wait timeout in milliseconds.
     * <p>
     * Only applicable when wait strategy is {@link WaitStrategyType#TIMEOUT_BLOCKING}.
     *
     * @param waitTimeoutMs wait timeout in milliseconds
     * @return this options instance for chaining
     */
    public EventBusOptions setWaitTimeoutMs(final int waitTimeoutMs) {
        Requires.requireTrue(waitTimeoutMs > 0, "waitTimeoutMs must be positive");
        this.waitTimeoutMs = waitTimeoutMs;
        return this;
    }

    @Override
    public String toString() {
        return "EventBusOptions{" + "mode=" + this.mode + ", bufferSize=" + this.bufferSize + ", maxBatchSize="
               + this.maxBatchSize + ", name='" + this.name + '\'' + ", waitStrategy=" + this.waitStrategy
               + ", waitTimeoutMs=" + this.waitTimeoutMs + '}';
    }
}
