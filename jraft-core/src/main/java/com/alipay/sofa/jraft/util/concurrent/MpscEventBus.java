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

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.jctools.queues.atomic.MpscGrowableAtomicArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.util.Requires;

/**
 * MPSC (Multi-Producer Single-Consumer) queue based {@link EventBus} implementation
 * without object reuse.
 * <p>
 * This implementation avoids cross-generation reference issues by not reusing
 * event objects. Each event is a new allocation that gets garbage collected
 * after processing, which is more efficient for generational GC.
 *
 * @param <T> the event type
 * @author dennis
 * @see <a href="https://github.com/sofastack/sofa-jraft/issues/1231">Issue #1231</a>
 */
public class MpscEventBus<T> implements EventBus<T> {

    private static final Logger    LOG                 = LoggerFactory.getLogger(MpscEventBus.class);
    private static final int       INITIAL_CAPACITY    = 1024;
    private static final long      BLOCKING_PARK_NANOS = 1_000_000L;                                 // 1ms
    private static final long      SPIN_PARK_NANOS     = 1_000L;                                     // 1 microsecond

    private final Queue<Object>    queue;
    private final Thread           consumerThread;
    private final String           name;
    private final int              bufferSize;
    private final int              maxBatchSize;
    private final WaitStrategyType waitStrategy;
    private final long             waitTimeoutNanos;
    private volatile boolean       shutdown;

    /**
     * Shutdown signal event.
     */
    private static class ShutdownEvent {
        final CountDownLatch latch;

        ShutdownEvent(final CountDownLatch latch) {
            this.latch = latch;
        }
    }

    public MpscEventBus(final EventBusOptions opts, final EventBusHandler<T> handler) {
        Requires.requireNonNull(opts, "opts");
        Requires.requireNonNull(handler, "handler");

        this.name = opts.getName();
        this.bufferSize = opts.getBufferSize();
        this.maxBatchSize = opts.getMaxBatchSize();
        this.waitStrategy = opts.getWaitStrategy();
        this.waitTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(opts.getWaitTimeoutMs());
        this.shutdown = false;

        // Use bounded MPSC queue
        // initialCapacity must be less than maxCapacity for MpscGrowableAtomicArrayQueue
        final int initialCapacity = Math.min(INITIAL_CAPACITY, this.bufferSize / 2);
        this.queue = new MpscGrowableAtomicArrayQueue<>(Math.max(2, initialCapacity), this.bufferSize);

        this.consumerThread = opts.getThreadFactory().newThread(() -> consumeLoop(handler));
        this.consumerThread.start();

        LOG.info("MpscEventBus '{}' started with bufferSize={}, maxBatchSize={}", this.name, this.bufferSize,
            this.maxBatchSize);
    }

    @Override
    public boolean publish(final T event) {
        if (this.shutdown) {
            LOG.warn("EventBus '{}' is shutdown, cannot publish event", this.name);
            return false;
        }

        // Blocking mode: spin until space available
        while (!this.queue.offer(event)) {
            if (this.shutdown) {
                return false;
            }
            LockSupport.parkNanos(SPIN_PARK_NANOS);
        }
        LockSupport.unpark(this.consumerThread);
        return true;
    }

    @Override
    public boolean tryPublish(final T event) {
        if (this.shutdown) {
            return false;
        }
        final boolean success = this.queue.offer(event);
        if (success) {
            LockSupport.unpark(this.consumerThread);
        }
        return success;
    }

    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity) {
        // Approximate check for bounded queue
        return this.queue.size() + requiredCapacity <= this.bufferSize;
    }

    @Override
    public int getBufferSize() {
        return this.bufferSize;
    }

    @Override
    public int getPendingEventCount() {
        return this.queue.size();
    }

    @Override
    public void shutdown(final CountDownLatch latch) {
        this.shutdown = true;
        // Spin until ShutdownEvent is successfully added
        while (!this.queue.offer(new ShutdownEvent(latch))) {
            LockSupport.parkNanos(SPIN_PARK_NANOS);
        }
        LockSupport.unpark(this.consumerThread);
    }

    @Override
    public boolean isShutdown() {
        return this.shutdown;
    }

    @SuppressWarnings("unchecked")
    private void consumeLoop(final EventBusHandler<T> handler) {
        int processedInBatch = 0;

        while (true) {
            final Object item = this.queue.poll();

            if (item == null) {
                // Queue is empty
                if (this.shutdown) {
                    // Shutdown requested and queue is empty, exit
                    return;
                }
                // Reset batch counter and wait for new events
                processedInBatch = 0;
                parkWait();
                continue;
            }

            if (item instanceof ShutdownEvent) {
                LOG.info("EventBus '{}' received shutdown signal, draining remaining events", this.name);
                drainAndProcess(handler);
                ((ShutdownEvent) item).latch.countDown();
                return;
            }

            final T event = (T) item;
            processedInBatch++;

            // endOfBatch: queue empty OR reached max batch size
            final boolean endOfBatch = this.queue.isEmpty() || processedInBatch >= this.maxBatchSize;

            try {
                handler.onEvent(event, endOfBatch);
            } catch (final Exception e) {
                LOG.error("Error processing event in EventBus '{}'", this.name, e);
            }

            if (endOfBatch) {
                processedInBatch = 0;
            }
            // No need to clear reference - object is not reused and will be GC'd naturally
        }
    }

    @SuppressWarnings("unchecked")
    private void drainAndProcess(final EventBusHandler<T> handler) {
        Object item;
        int processedInBatch = 0;

        while ((item = this.queue.poll()) != null) {
            if (item instanceof ShutdownEvent) {
                // Ignore additional shutdown events during drain
                continue;
            }

            final T event = (T) item;
            processedInBatch++;
            final boolean endOfBatch = this.queue.isEmpty() || processedInBatch >= this.maxBatchSize;

            try {
                handler.onEvent(event, endOfBatch);
            } catch (final Exception e) {
                LOG.error("Error processing event during drain in EventBus '{}'", this.name, e);
            }

            if (endOfBatch) {
                processedInBatch = 0;
            }
        }
    }

    private void parkWait() {
        if (this.waitStrategy == WaitStrategyType.TIMEOUT_BLOCKING) {
            LockSupport.parkNanos(this.waitTimeoutNanos);
        } else {
            LockSupport.parkNanos(BLOCKING_PARK_NANOS);
        }
    }
}
