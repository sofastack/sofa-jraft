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

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for EventBusFactory.
 *
 * @author dennis
 */
public class EventBusFactoryTest {

    @Test
    public void testCreateDisruptorEventBus() throws InterruptedException {
        final EventBusOptions opts = new EventBusOptions()
            .setMode(EventBusMode.DISRUPTOR)
            .setName("test-disruptor")
            .setBufferSize(1024);

        final EventBus<String> eventBus = EventBusFactory.create(opts, (event, endOfBatch) -> {});

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

        final EventBus<String> eventBus = EventBusFactory.create(opts, (event, endOfBatch) -> {});

        assertNotNull(eventBus);
        assertTrue(eventBus instanceof MpscEventBus);

        final CountDownLatch latch = new CountDownLatch(1);
        eventBus.shutdown(latch);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test(expected = NullPointerException.class)
    public void testCreateWithNullOptions() {
        EventBusFactory.create(null, (event, endOfBatch) -> {});
    }

    @Test(expected = NullPointerException.class)
    public void testCreateWithNullHandler() {
        final EventBusOptions opts = new EventBusOptions();
        EventBusFactory.create(opts, null);
    }

    @Test
    public void testFactoryRespectsMode() throws InterruptedException {
        // Test DISRUPTOR mode
        EventBusOptions opts = new EventBusOptions().setMode(EventBusMode.DISRUPTOR);
        EventBus<String> bus = EventBusFactory.create(opts, (e, b) -> {});
        assertTrue("Expected DisruptorEventBus for DISRUPTOR mode", bus instanceof DisruptorEventBus);
        CountDownLatch latch = new CountDownLatch(1);
        bus.shutdown(latch);
        latch.await(5, TimeUnit.SECONDS);

        // Test MPSC mode
        opts = new EventBusOptions().setMode(EventBusMode.MPSC);
        bus = EventBusFactory.create(opts, (e, b) -> {});
        assertTrue("Expected MpscEventBus for MPSC mode", bus instanceof MpscEventBus);
        latch = new CountDownLatch(1);
        bus.shutdown(latch);
        latch.await(5, TimeUnit.SECONDS);
    }
}
