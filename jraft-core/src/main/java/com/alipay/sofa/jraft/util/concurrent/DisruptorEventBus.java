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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.util.DisruptorBuilder;
import com.alipay.sofa.jraft.util.Requires;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Disruptor-based {@link EventBus} implementation with object reuse.
 * <p>
 * This implementation uses Disruptor's RingBuffer for high-performance
 * event processing. Events are wrapped in {@link EventWrapper} objects
 * that are reused, which may cause cross-generation reference issues
 * with generational GC.
 *
 * @param <T> the event type
 * @author dennis
 */
public class DisruptorEventBus<T> implements EventBus<T> {

    private static final Logger               LOG = LoggerFactory.getLogger(DisruptorEventBus.class);

    private final Disruptor<EventWrapper<T>>  disruptor;
    private final RingBuffer<EventWrapper<T>> ringBuffer;
    private final String                      name;
    private volatile boolean                  shutdown;

    /**
     * Wrapper class to hold event data in RingBuffer.
     */
    private static class EventWrapper<T> {
        T              event;
        CountDownLatch shutdownLatch;

        void reset() {
            this.event = null;
            this.shutdownLatch = null;
        }
    }

    @SuppressWarnings("unchecked")
    public DisruptorEventBus(final EventBusOptions opts, final EventBusHandler<T> handler) {
        Requires.requireNonNull(opts, "opts");
        Requires.requireNonNull(handler, "handler");

        this.name = opts.getName();
        this.shutdown = false;

        final WaitStrategy waitStrategy = createWaitStrategy(opts);

        this.disruptor = DisruptorBuilder.<EventWrapper<T>> newInstance()
            .setEventFactory(EventWrapper::new)
            .setRingBufferSize(opts.getBufferSize())
            .setThreadFactory(opts.getThreadFactory())
            .setProducerType(ProducerType.MULTI)
            .setWaitStrategy(waitStrategy)
            .build();

        this.disruptor.handleEventsWith(new InternalHandler(handler));
        this.ringBuffer = this.disruptor.start();

        LOG.info("DisruptorEventBus '{}' started with bufferSize={}", this.name, opts.getBufferSize());
    }

    @Override
    public boolean publish(final T event) {
        if (this.shutdown) {
            LOG.warn("EventBus '{}' is shutdown, cannot publish event", this.name);
            return false;
        }
        this.ringBuffer.publishEvent((wrapper, seq) -> wrapper.event = event);
        return true;
    }

    @Override
    public boolean tryPublish(final T event) {
        if (this.shutdown) {
            return false;
        }
        return this.ringBuffer.tryPublishEvent((wrapper, seq) -> wrapper.event = event);
    }

    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity) {
        return this.ringBuffer.hasAvailableCapacity(requiredCapacity);
    }

    @Override
    public int getBufferSize() {
        return this.ringBuffer.getBufferSize();
    }

    @Override
    public int getPendingEventCount() {
        return this.ringBuffer.getBufferSize() - (int) this.ringBuffer.remainingCapacity();
    }

    @Override
    public void shutdown(final CountDownLatch latch) {
        this.shutdown = true;
        this.ringBuffer.publishEvent((wrapper, seq) -> wrapper.shutdownLatch = latch);
    }

    @Override
    public boolean isShutdown() {
        return this.shutdown;
    }

    private static WaitStrategy createWaitStrategy(final EventBusOptions opts) {
        switch (opts.getWaitStrategy()) {
            case TIMEOUT_BLOCKING:
                return new TimeoutBlockingWaitStrategy(opts.getWaitTimeoutMs(), TimeUnit.MILLISECONDS);
            case BLOCKING:
            default:
                return new BlockingWaitStrategy();
        }
    }

    /**
     * Internal handler that adapts Disruptor's EventHandler to our EventBusHandler.
     */
    private class InternalHandler implements com.lmax.disruptor.EventHandler<EventWrapper<T>> {

        private final EventBusHandler<T> userHandler;

        InternalHandler(final EventBusHandler<T> userHandler) {
            this.userHandler = userHandler;
        }

        @Override
        public void onEvent(final EventWrapper<T> wrapper, final long sequence, final boolean endOfBatch)
                                                                                                         throws Exception {
            try {
                if (wrapper.shutdownLatch != null) {
                    LOG.info("EventBus '{}' received shutdown signal", DisruptorEventBus.this.name);
                    wrapper.shutdownLatch.countDown();
                    DisruptorEventBus.this.disruptor.shutdown();
                    return;
                }

                this.userHandler.onEvent(wrapper.event, endOfBatch);
            } finally {
                // Clear reference after processing to help GC
                wrapper.reset();
            }
        }
    }
}
