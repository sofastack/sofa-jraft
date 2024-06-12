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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

public class ThreadIdTest implements ThreadId.OnError {
    private ThreadId     id;
    private volatile int errorCode = -1;

    @Override
    public void onError(final ThreadId id, final Object data, final int errorCode) {
        assertSame(id, this.id);
        this.errorCode = errorCode;
    }

    @Before
    public void setup() {
        this.id = new ThreadId(this, this);
    }

    @Test
    public void testLockUnlock() throws Exception {
        assertSame(this, this.id.lock());
        AtomicLong cost = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                ThreadIdTest.this.id.lock();
                cost.set(System.currentTimeMillis() - start);
                latch.countDown();
            }
        }.start();
        Thread.sleep(1000);
        this.id.unlock();
        latch.await();
        assertEquals(1000, cost.get(), 20);
    }

    @Test
    public void testSetError() throws Exception {
        this.id.setError(100);
        assertEquals(100, this.errorCode);
        CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                ThreadIdTest.this.id.setError(99);
                latch.countDown();
            }
        }.start();
        latch.await();
        assertEquals(99, this.errorCode);
    }

    @Test
    public void testUnlockAndDestroy() throws Exception {
        AtomicInteger lockSuccess = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(10);
        this.id.lock();
        for (int i = 0; i < 10; i++) {
            new Thread() {
                @Override
                public void run() {
                    if (ThreadIdTest.this.id.lock() != null) {
                        lockSuccess.incrementAndGet();
                    }
                    latch.countDown();
                }
            }.start();
        }
        this.id.unlockAndDestroy();
        latch.await();
        assertEquals(0, lockSuccess.get());
        assertNull(this.id.lock());
    }
}
