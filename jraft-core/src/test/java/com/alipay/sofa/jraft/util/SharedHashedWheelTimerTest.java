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
package com.alipay.sofa.jraft.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.util.timer.SharedHashedWheelTimer;
import com.alipay.sofa.jraft.util.timer.Timeout;
import com.alipay.sofa.jraft.util.timer.Timer;
import com.alipay.sofa.jraft.util.timer.TimerTask;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

/**
 * @author zongtanghu
 */
public class SharedHashedWheelTimerTest {

    private Timer        sharedHashedWheelTimer;

    private final int    workerNum = Utils.cpus();
    private final String name      = "ShareHashedWheelTimer";

    @Before
    public void setup() {
        this.sharedHashedWheelTimer = new SharedHashedWheelTimer(this.workerNum, this.name);
    }

    @Test
    public void testScheduleTimeoutShouldRunAfterDelay() throws InterruptedException {
        final CountDownLatch barrier = new CountDownLatch(1);
        final Timeout timeout = this.sharedHashedWheelTimer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                barrier.countDown();
            }
        }, 2, TimeUnit.SECONDS);

        assertTrue(barrier.await(3, TimeUnit.SECONDS));
        assertTrue("timer should expire", timeout.isExpired());
    }

    @Test
    public void testScheduleTimeoutShouldNotRunBeforeDelay() throws InterruptedException {
        final CountDownLatch barrier = new CountDownLatch(1);
        final Timeout timeout = this.sharedHashedWheelTimer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                fail("This should not have run");
                barrier.countDown();
            }
        }, 10, TimeUnit.SECONDS);
        assertFalse(barrier.await(3, TimeUnit.SECONDS));
        assertFalse("timer should not expire", timeout.isExpired());
    }

    @Test(timeout = 3000)
    public void testStopTimer() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(3);
        for (int i = 0; i < 3; i++) {
            this.sharedHashedWheelTimer.newTimeout(new TimerTask() {
                @Override
                public void run(final Timeout timeout) throws Exception {
                    latch.countDown();
                }
            }, 1, TimeUnit.MILLISECONDS);
        }

        latch.await();
        assertEquals("Number of unprocessed timeouts should be 0", 0, this.sharedHashedWheelTimer.stop().size());

        this.sharedHashedWheelTimer = new SharedHashedWheelTimer(this.workerNum, this.name);
        for (int i = 0; i < 5; i++) {
            this.sharedHashedWheelTimer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                }
            }, 5, TimeUnit.SECONDS);
        }
        Thread.sleep(1000L); // sleep for a second
        assertFalse("Number of unprocessed timeouts should be greater than 0", this.sharedHashedWheelTimer.stop()
            .isEmpty());
    }

    @Test
    public void testExecutionOnTime() throws InterruptedException {
        int tickDuration = 200;
        int timeout = 125;
        int maxTimeout = 2 * (tickDuration + timeout);
        final BlockingQueue<Long> queue = new LinkedBlockingQueue<Long>();

        int scheduledTasks = 100000;
        for (int i = 0; i < scheduledTasks; i++) {
            final long start = System.nanoTime();
            this.sharedHashedWheelTimer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    queue.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                }
            }, timeout, TimeUnit.MILLISECONDS);
        }

        for (int i = 0; i < scheduledTasks; i++) {
            long delay = queue.take();
            assertTrue("Timeout + " + scheduledTasks + " delay " + delay + " must be " + timeout + " < " + maxTimeout,
                delay >= timeout && delay < maxTimeout);
        }
    }

    @After
    public void teardown() {
        this.sharedHashedWheelTimer.stop();
    }

}
