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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests specific to MpscEventBus, focusing on maxBatchSize behavior.
 *
 * @author dennis
 */
public class MpscEventBusTest {

    private MpscEventBus<String> eventBus;

    @After
    public void teardown() throws InterruptedException {
        if (this.eventBus != null && !this.eventBus.isShutdown()) {
            final CountDownLatch latch = new CountDownLatch(1);
            this.eventBus.shutdown(latch);
            latch.await(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Test that endOfBatch is triggered when maxBatchSize is reached.
     */
    @Test
    public void testMaxBatchSizeTrigger() throws InterruptedException {
        final int maxBatchSize = 5;
        final int totalEvents = 20;
        final List<Boolean> endOfBatchFlags = new ArrayList<>();
        final CountDownLatch processLatch = new CountDownLatch(totalEvents);

        final EventBusOptions opts = new EventBusOptions()
            .setMode(EventBusMode.MPSC)
            .setName("test-batch")
            .setBufferSize(1024)
            .setMaxBatchSize(maxBatchSize);

        this.eventBus = new MpscEventBus<>(opts, (event, endOfBatch) -> {
            synchronized (endOfBatchFlags) {
                endOfBatchFlags.add(endOfBatch);
            }
            processLatch.countDown();
        });

        // Publish all events quickly to ensure batching
        for (int i = 0; i < totalEvents; i++) {
            assertTrue(this.eventBus.publish("event" + i));
        }

        assertTrue(processLatch.await(5, TimeUnit.SECONDS));

        // Give a bit more time for all flags to be recorded
        Thread.sleep(100);

        synchronized (endOfBatchFlags) {
            assertEquals(totalEvents, endOfBatchFlags.size());

            // Count endOfBatch=true occurrences
            int endOfBatchCount = 0;
            for (final Boolean flag : endOfBatchFlags) {
                if (flag) {
                    endOfBatchCount++;
                }
            }

            // Should have at least totalEvents/maxBatchSize endOfBatch=true flags
            // (could have more if queue becomes empty during processing)
            assertTrue("Expected at least " + (totalEvents / maxBatchSize) + " endOfBatch=true, got " + endOfBatchCount,
                endOfBatchCount >= totalEvents / maxBatchSize);
        }
    }

    /**
     * Test that maxBatchSize=1 causes every event to be endOfBatch.
     */
    @Test
    public void testMaxBatchSizeOne() throws InterruptedException {
        final int totalEvents = 10;
        final List<Boolean> endOfBatchFlags = new ArrayList<>();
        final CountDownLatch processLatch = new CountDownLatch(totalEvents);

        final EventBusOptions opts = new EventBusOptions()
            .setMode(EventBusMode.MPSC)
            .setName("test-batch-one")
            .setBufferSize(1024)
            .setMaxBatchSize(1);

        this.eventBus = new MpscEventBus<>(opts, (event, endOfBatch) -> {
            synchronized (endOfBatchFlags) {
                endOfBatchFlags.add(endOfBatch);
            }
            processLatch.countDown();
        });

        for (int i = 0; i < totalEvents; i++) {
            assertTrue(this.eventBus.publish("event" + i));
        }

        assertTrue(processLatch.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);

        synchronized (endOfBatchFlags) {
            assertEquals(totalEvents, endOfBatchFlags.size());
            // Every event should be endOfBatch when maxBatchSize=1
            for (final Boolean flag : endOfBatchFlags) {
                assertTrue(flag);
            }
        }
    }

    /**
     * Test that slow events still trigger endOfBatch based on queue empty.
     */
    @Test
    public void testEndOfBatchOnEmptyQueue() throws InterruptedException {
        final List<Boolean> endOfBatchFlags = new ArrayList<>();
        final AtomicInteger eventCount = new AtomicInteger(0);

        final EventBusOptions opts = new EventBusOptions()
            .setMode(EventBusMode.MPSC)
            .setName("test-empty-queue")
            .setBufferSize(1024)
            .setMaxBatchSize(100); // Large batch size

        this.eventBus = new MpscEventBus<>(opts, (event, endOfBatch) -> {
            synchronized (endOfBatchFlags) {
                endOfBatchFlags.add(endOfBatch);
            }
            eventCount.incrementAndGet();
        });

        // Publish events one by one with delay to ensure queue is empty each time
        for (int i = 0; i < 3; i++) {
            assertTrue(this.eventBus.publish("event" + i));
            Thread.sleep(50); // Wait for processing
        }

        // Wait for all events to be processed
        while (eventCount.get() < 3) {
            Thread.sleep(10);
        }
        Thread.sleep(100);

        synchronized (endOfBatchFlags) {
            assertEquals(3, endOfBatchFlags.size());
            // Each event should be endOfBatch since queue is empty after each
            for (final Boolean flag : endOfBatchFlags) {
                assertTrue("Expected endOfBatch=true for spaced-out events", flag);
            }
        }
    }

    /**
     * Test drain on shutdown processes remaining events.
     */
    @Test
    public void testDrainOnShutdown() throws InterruptedException {
        final AtomicInteger processedCount = new AtomicInteger(0);

        final EventBusOptions opts = new EventBusOptions()
            .setMode(EventBusMode.MPSC)
            .setName("test-drain")
            .setBufferSize(1024)
            .setMaxBatchSize(32);

        this.eventBus = new MpscEventBus<>(opts, (event, endOfBatch) -> {
            processedCount.incrementAndGet();
            // Slow processing to ensure events queue up
            try {
                Thread.sleep(10);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Publish many events quickly
        final int totalEvents = 50;
        for (int i = 0; i < totalEvents; i++) {
            this.eventBus.publish("event" + i);
        }

        // Immediately shutdown - should drain remaining events
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        this.eventBus.shutdown(shutdownLatch);
        assertTrue(shutdownLatch.await(10, TimeUnit.SECONDS));

        // All events should be processed
        assertEquals(totalEvents, processedCount.get());
    }
}
