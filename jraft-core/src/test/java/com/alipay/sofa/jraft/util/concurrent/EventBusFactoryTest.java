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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.util.JRaftServiceLoader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for EventBusFactory.
 *
 * @author dennis
 */
public class EventBusFactoryTest {

    private final EventBusFactory factory = new DefaultEventBusFactory();

    @Test
    public void testCreateDisruptorEventBus() throws InterruptedException {
        final EventBusOptions opts = new EventBusOptions()
            .setMode(EventBusMode.DISRUPTOR)
            .setName("test-disruptor")
            .setBufferSize(1024);

        final EventBus<String> eventBus = this.factory.create(opts, (event, endOfBatch) -> {});

        assertNotNull(eventBus);
        assertTrue(eventBus instanceof DisruptorEventBus);

        final CountDownLatch latch = new CountDownLatch(1);
        eventBus.shutdown(latch);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testCreateMpscEventBus() throws InterruptedException {
        final EventBusOptions opts = new EventBusOptions()
            .setMode(EventBusMode.MPSC)
            .setName("test-mpsc")
            .setBufferSize(1024);

        final EventBus<String> eventBus = this.factory.create(opts, (event, endOfBatch) -> {});

        assertNotNull(eventBus);
        assertTrue(eventBus instanceof MpscEventBus);

        final CountDownLatch latch = new CountDownLatch(1);
        eventBus.shutdown(latch);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test(expected = NullPointerException.class)
    public void testCreateWithNullOptions() {
        this.factory.create(null, (event, endOfBatch) -> {});
    }

    @Test(expected = NullPointerException.class)
    public void testCreateWithNullHandler() {
        final EventBusOptions opts = new EventBusOptions();
        this.factory.create(opts, null);
    }

    @Test
    public void testFactoryRespectsMode() throws InterruptedException {
        // Test DISRUPTOR mode
        EventBusOptions opts = new EventBusOptions().setMode(EventBusMode.DISRUPTOR);
        EventBus<String> bus = this.factory.create(opts, (e, b) -> {});
        assertTrue("Expected DisruptorEventBus for DISRUPTOR mode", bus instanceof DisruptorEventBus);
        CountDownLatch latch = new CountDownLatch(1);
        bus.shutdown(latch);
        latch.await(5, TimeUnit.SECONDS);

        // Test MPSC mode
        opts = new EventBusOptions().setMode(EventBusMode.MPSC);
        bus = this.factory.create(opts, (e, b) -> {});
        assertTrue("Expected MpscEventBus for MPSC mode", bus instanceof MpscEventBus);
        latch = new CountDownLatch(1);
        bus.shutdown(latch);
        latch.await(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSpiLoadsDefaultFactory() {
        final EventBusFactory spiFactory = JRaftServiceLoader.load(EventBusFactory.class).first();
        assertNotNull("SPI should load EventBusFactory", spiFactory);
        assertTrue("Default factory should be DefaultEventBusFactory", spiFactory instanceof DefaultEventBusFactory);
    }

    @Test
    public void testCustomFactoryImplementation() throws InterruptedException {
        final AtomicInteger createCount = new AtomicInteger(0);

        // Custom factory that wraps default behavior and counts invocations
        final EventBusFactory customFactory = new EventBusFactory() {
            private final EventBusFactory delegate = new DefaultEventBusFactory();

            @Override
            public <T> EventBus<T> create(final EventBusOptions opts, final EventBusHandler<T> handler) {
                createCount.incrementAndGet();
                return this.delegate.create(opts, handler);
            }
        };

        final EventBusOptions opts = new EventBusOptions()
            .setMode(EventBusMode.MPSC)
            .setName("custom-test");

        final EventBus<String> bus = customFactory.create(opts, (e, b) -> {});
        assertNotNull(bus);
        assertEquals(1, createCount.get());

        final CountDownLatch latch = new CountDownLatch(1);
        bus.shutdown(latch);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testRaftOptionsDefaultEventBusFactory() {
        final RaftOptions opts = new RaftOptions();
        assertNotNull("Default eventBusFactory should not be null", opts.getEventBusFactory());
        assertTrue("Default should be DefaultEventBusFactory",
            opts.getEventBusFactory() instanceof DefaultEventBusFactory);
    }

    @Test
    public void testRaftOptionsSetCustomEventBusFactory() {
        final RaftOptions opts = new RaftOptions();
        final EventBusFactory customFactory = new DefaultEventBusFactory();

        opts.setEventBusFactory(customFactory);
        assertSame("Should return the same factory instance", customFactory, opts.getEventBusFactory());
    }

    @Test
    public void testRaftOptionsCopyPreservesEventBusFactory() {
        final RaftOptions opts = new RaftOptions();
        final EventBusFactory customFactory = new DefaultEventBusFactory();
        opts.setEventBusFactory(customFactory);

        final RaftOptions copied = opts.copy();
        assertSame("Copied options should have same factory", customFactory, copied.getEventBusFactory());
    }

    @Test
    public void testRaftOptionsCopyPreservesEventBusMode() {
        final RaftOptions opts = new RaftOptions();
        opts.setEventBusMode(EventBusMode.MPSC);

        final RaftOptions copied = opts.copy();
        assertEquals("Copied options should have same mode", EventBusMode.MPSC, copied.getEventBusMode());
    }

    @Test
    public void testCustomFactoryUsedByRaftOptions() throws InterruptedException {
        final AtomicInteger createCount = new AtomicInteger(0);

        final EventBusFactory customFactory = new EventBusFactory() {
            private final EventBusFactory delegate = new DefaultEventBusFactory();

            @Override
            public <T> EventBus<T> create(final EventBusOptions opts, final EventBusHandler<T> handler) {
                createCount.incrementAndGet();
                return this.delegate.create(opts, handler);
            }
        };

        final RaftOptions raftOptions = new RaftOptions();
        raftOptions.setEventBusFactory(customFactory);

        // Simulate what NodeImpl/FSMCallerImpl/LogManagerImpl do
        final EventBusOptions eventBusOpts = new EventBusOptions()
            .setMode(raftOptions.getEventBusMode())
            .setName("test-custom")
            .setBufferSize(raftOptions.getDisruptorBufferSize());

        final EventBus<String> bus = raftOptions.getEventBusFactory().create(eventBusOpts, (e, b) -> {});
        assertNotNull(bus);
        assertEquals("Custom factory should be invoked", 1, createCount.get());

        final CountDownLatch latch = new CountDownLatch(1);
        bus.shutdown(latch);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }
}
