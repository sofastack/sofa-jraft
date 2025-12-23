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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for EventBus implementations (DisruptorEventBus and MpscEventBus).
 *
 * @author dennis
 */
@RunWith(Parameterized.class)
public class EventBusTest {

    @Parameterized.Parameters(name = "EventBusMode={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { EventBusMode.DISRUPTOR }, { EventBusMode.MPSC } });
    }

    @Parameterized.Parameter
    public EventBusMode      mode;

    private EventBus<String> eventBus;
    private List<String>     receivedEvents;
    private List<Boolean>    endOfBatchFlags;
    private CountDownLatch   processLatch;

    @Before
    public void setup() {
        this.receivedEvents = new ArrayList<>();
        this.endOfBatchFlags = new ArrayList<>();
    }

    @After
    public void teardown() throws InterruptedException {
        if (this.eventBus != null && !this.eventBus.isShutdown()) {
            final CountDownLatch latch = new CountDownLatch(1);
            this.eventBus.shutdown(latch);
            latch.await(5, TimeUnit.SECONDS);
        }
    }

    private final EventBusFactory factory = new DefaultEventBusFactory();

    private EventBus<String> createEventBus(final int bufferSize, final int maxBatchSize) {
        final EventBusOptions opts = new EventBusOptions()
            .setMode(this.mode)
            .setName("test-eventbus")
            .setBufferSize(bufferSize)
            .setMaxBatchSize(maxBatchSize);

        return this.factory.create(opts, (event, endOfBatch) -> {
            synchronized (this.receivedEvents) {
                this.receivedEvents.add(event);
                this.endOfBatchFlags.add(endOfBatch);
            }
            if (this.processLatch != null) {
                this.processLatch.countDown();
            }
        });
    }

    @Test
    public void testPublishAndConsume() throws InterruptedException {
        this.processLatch = new CountDownLatch(3);
        this.eventBus = createEventBus(1024, 32);

        assertTrue(this.eventBus.publish("event1"));
        assertTrue(this.eventBus.publish("event2"));
        assertTrue(this.eventBus.publish("event3"));

        assertTrue(this.processLatch.await(5, TimeUnit.SECONDS));

        synchronized (this.receivedEvents) {
            assertEquals(3, this.receivedEvents.size());
            assertTrue(this.receivedEvents.contains("event1"));
            assertTrue(this.receivedEvents.contains("event2"));
            assertTrue(this.receivedEvents.contains("event3"));
        }
    }

    @Test
    public void testTryPublish() throws InterruptedException {
        this.processLatch = new CountDownLatch(2);
        this.eventBus = createEventBus(1024, 32);

        assertTrue(this.eventBus.tryPublish("event1"));
        assertTrue(this.eventBus.tryPublish("event2"));

        assertTrue(this.processLatch.await(5, TimeUnit.SECONDS));

        synchronized (this.receivedEvents) {
            assertEquals(2, this.receivedEvents.size());
        }
    }

    @Test
    public void testShutdown() throws InterruptedException {
        this.eventBus = createEventBus(1024, 32);

        assertFalse(this.eventBus.isShutdown());

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        this.eventBus.shutdown(shutdownLatch);

        assertTrue(shutdownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(this.eventBus.isShutdown());

        // After shutdown, publish should fail
        assertFalse(this.eventBus.publish("should-fail"));
        assertFalse(this.eventBus.tryPublish("should-fail"));
    }

    @Test
    public void testGetBufferSize() {
        this.eventBus = createEventBus(2048, 32);
        assertEquals(2048, this.eventBus.getBufferSize());
    }

    @Test
    public void testHasAvailableCapacity() throws InterruptedException {
        this.eventBus = createEventBus(1024, 32);

        assertTrue(this.eventBus.hasAvailableCapacity(1));
        assertTrue(this.eventBus.hasAvailableCapacity(100));
    }

    @Test
    public void testEndOfBatchSingleEvent() throws InterruptedException {
        this.processLatch = new CountDownLatch(1);
        this.eventBus = createEventBus(1024, 32);

        assertTrue(this.eventBus.publish("single"));

        assertTrue(this.processLatch.await(5, TimeUnit.SECONDS));

        // Give time for event processing to complete
        Thread.sleep(100);

        synchronized (this.receivedEvents) {
            assertEquals(1, this.receivedEvents.size());
            // Single event should be end of batch
            assertTrue(this.endOfBatchFlags.get(0));
        }
    }

    @Test
    public void testConcurrentPublish() throws InterruptedException {
        final int threads = 10;
        final int eventsPerThread = 100;
        this.processLatch = new CountDownLatch(threads * eventsPerThread);
        this.eventBus = createEventBus(4096, 32);

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(threads);
        final AtomicInteger publishedCount = new AtomicInteger(0);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < eventsPerThread; i++) {
                        if (this.eventBus.publish("t" + threadId + "-e" + i)) {
                            publishedCount.incrementAndGet();
                        }
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        assertTrue(this.processLatch.await(10, TimeUnit.SECONDS));

        assertEquals(threads * eventsPerThread, publishedCount.get());
        synchronized (this.receivedEvents) {
            assertEquals(threads * eventsPerThread, this.receivedEvents.size());
        }
    }

    @Test
    public void testPublishAfterShutdownFails() throws InterruptedException {
        this.eventBus = createEventBus(1024, 32);

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        this.eventBus.shutdown(shutdownLatch);
        assertTrue(shutdownLatch.await(5, TimeUnit.SECONDS));
        assertTrue("EventBus should be shutdown", this.eventBus.isShutdown());

        // Give a small delay to ensure shutdown flag is visible across threads
        Thread.sleep(50);

        assertFalse("publish should return false after shutdown", this.eventBus.publish("after-shutdown"));
        assertFalse("tryPublish should return false after shutdown", this.eventBus.tryPublish("after-shutdown"));
    }

    @Test
    public void testGetPendingEventCount() throws InterruptedException {
        // Block the consumer so events accumulate
        final CountDownLatch blockLatch = new CountDownLatch(1);
        final CountDownLatch firstEventLatch = new CountDownLatch(1);

        final EventBusOptions opts = new EventBusOptions()
            .setMode(this.mode)
            .setName("test-pending-count")
            .setBufferSize(1024)
            .setMaxBatchSize(32);

        final EventBus<String> blockingBus = this.factory.create(opts, (event, endOfBatch) -> {
            firstEventLatch.countDown();
            try {
                blockLatch.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        try {
            // Initially empty
            assertEquals(0, blockingBus.getPendingEventCount());

            // Publish first event to block the consumer
            assertTrue(blockingBus.publish("block"));
            assertTrue(firstEventLatch.await(5, TimeUnit.SECONDS));

            // Now publish more events that will queue up
            for (int i = 0; i < 5; i++) {
                assertTrue(blockingBus.publish("queued-" + i));
            }

            // Should have pending events (at least the 5 we just added)
            assertTrue("Should have pending events", blockingBus.getPendingEventCount() >= 5);

            // Unblock and shutdown
            blockLatch.countDown();
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            blockingBus.shutdown(shutdownLatch);
            assertTrue(shutdownLatch.await(5, TimeUnit.SECONDS));
        } finally {
            blockLatch.countDown(); // Ensure unblocked even if test fails
        }
    }
}
