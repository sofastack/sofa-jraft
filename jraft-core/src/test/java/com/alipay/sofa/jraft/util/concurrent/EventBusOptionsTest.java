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

import org.junit.Test;

import com.alipay.sofa.jraft.util.NamedThreadFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for EventBusOptions.
 *
 * @author dennis
 */
public class EventBusOptionsTest {

    @Test
    public void testDefaultValues() {
        final EventBusOptions opts = new EventBusOptions();

        assertEquals(EventBusMode.DISRUPTOR, opts.getMode());
        assertEquals(16384, opts.getBufferSize());
        assertEquals(32, opts.getMaxBatchSize());
        assertEquals("EventBus", opts.getName());
        assertEquals(WaitStrategyType.BLOCKING, opts.getWaitStrategy());
        assertEquals(1000, opts.getWaitTimeoutMs());
        assertNotNull(opts.getThreadFactory());
    }

    @Test
    public void testSetMode() {
        final EventBusOptions opts = new EventBusOptions();

        opts.setMode(EventBusMode.MPSC);
        assertEquals(EventBusMode.MPSC, opts.getMode());

        opts.setMode(EventBusMode.DISRUPTOR);
        assertEquals(EventBusMode.DISRUPTOR, opts.getMode());
    }

    @Test
    public void testSetBufferSize() {
        final EventBusOptions opts = new EventBusOptions();

        opts.setBufferSize(4096);
        assertEquals(4096, opts.getBufferSize());

        opts.setBufferSize(1);
        assertEquals(1, opts.getBufferSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBufferSizeZero() {
        new EventBusOptions().setBufferSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBufferSizeNegative() {
        new EventBusOptions().setBufferSize(-1);
    }

    @Test
    public void testSetMaxBatchSize() {
        final EventBusOptions opts = new EventBusOptions();

        opts.setMaxBatchSize(64);
        assertEquals(64, opts.getMaxBatchSize());

        opts.setMaxBatchSize(1);
        assertEquals(1, opts.getMaxBatchSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetMaxBatchSizeZero() {
        new EventBusOptions().setMaxBatchSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetMaxBatchSizeNegative() {
        new EventBusOptions().setMaxBatchSize(-1);
    }

    @Test
    public void testSetName() {
        final EventBusOptions opts = new EventBusOptions();

        opts.setName("custom-bus");
        assertEquals("custom-bus", opts.getName());
    }

    @Test(expected = NullPointerException.class)
    public void testSetNameNull() {
        new EventBusOptions().setName(null);
    }

    @Test
    public void testSetThreadFactory() {
        final EventBusOptions opts = new EventBusOptions();
        final ThreadFactory factory = new NamedThreadFactory("test-", true);

        opts.setThreadFactory(factory);
        assertEquals(factory, opts.getThreadFactory());
    }

    @Test(expected = NullPointerException.class)
    public void testSetThreadFactoryNull() {
        new EventBusOptions().setThreadFactory(null);
    }

    @Test
    public void testSetWaitStrategy() {
        final EventBusOptions opts = new EventBusOptions();

        opts.setWaitStrategy(WaitStrategyType.TIMEOUT_BLOCKING);
        assertEquals(WaitStrategyType.TIMEOUT_BLOCKING, opts.getWaitStrategy());

        opts.setWaitStrategy(WaitStrategyType.BLOCKING);
        assertEquals(WaitStrategyType.BLOCKING, opts.getWaitStrategy());
    }

    @Test(expected = NullPointerException.class)
    public void testSetWaitStrategyNull() {
        new EventBusOptions().setWaitStrategy(null);
    }

    @Test
    public void testSetWaitTimeoutMs() {
        final EventBusOptions opts = new EventBusOptions();

        opts.setWaitTimeoutMs(5000);
        assertEquals(5000, opts.getWaitTimeoutMs());

        opts.setWaitTimeoutMs(1);
        assertEquals(1, opts.getWaitTimeoutMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetWaitTimeoutMsZero() {
        new EventBusOptions().setWaitTimeoutMs(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetWaitTimeoutMsNegative() {
        new EventBusOptions().setWaitTimeoutMs(-1);
    }

    @Test
    public void testFluentApi() {
        final ThreadFactory factory = new NamedThreadFactory("fluent-", true);

        final EventBusOptions opts = new EventBusOptions().setMode(EventBusMode.MPSC).setName("fluent-bus")
            .setBufferSize(2048).setMaxBatchSize(16).setThreadFactory(factory)
            .setWaitStrategy(WaitStrategyType.TIMEOUT_BLOCKING).setWaitTimeoutMs(2000);

        assertEquals(EventBusMode.MPSC, opts.getMode());
        assertEquals("fluent-bus", opts.getName());
        assertEquals(2048, opts.getBufferSize());
        assertEquals(16, opts.getMaxBatchSize());
        assertEquals(factory, opts.getThreadFactory());
        assertEquals(WaitStrategyType.TIMEOUT_BLOCKING, opts.getWaitStrategy());
        assertEquals(2000, opts.getWaitTimeoutMs());
    }

    @Test
    public void testToString() {
        final EventBusOptions opts = new EventBusOptions().setMode(EventBusMode.MPSC).setName("test-bus");

        final String str = opts.toString();
        assertNotNull(str);
        assertTrue(str.contains("MPSC"));
        assertTrue(str.contains("test-bus"));
    }

    private void assertTrue(final boolean condition) {
        org.junit.Assert.assertTrue(condition);
    }
}
