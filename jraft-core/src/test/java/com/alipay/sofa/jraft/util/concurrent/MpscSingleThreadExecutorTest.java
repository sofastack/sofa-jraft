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
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.alipay.sofa.jraft.util.NamedThreadFactory;

/**
 * @author jiachun.fjc
 */
public class MpscSingleThreadExecutorTest {

    private static final ThreadFactory THREAD_FACTORY = new NamedThreadFactory("test");

    @Test
    public void testExecutorIsShutdownWithoutTask() {
        final MpscSingleThreadExecutor executor = new MpscSingleThreadExecutor(1024, THREAD_FACTORY);

        Assert.assertTrue(executor.shutdownGracefully());
        executeShouldFail(executor);
        executeShouldFail(executor);
        Assert.assertTrue(executor.isShutdown());
    }

    @Test
    public void testExecutorIsShutdownWithTask() throws InterruptedException {
        final MpscSingleThreadExecutor executor = new MpscSingleThreadExecutor(1024, THREAD_FACTORY);
        final CountDownLatch latch = new CountDownLatch(10);
        final AtomicLong ret = new AtomicLong(0);
        for (int i = 0; i < 10; i++) {
            executor.execute(() -> {
                try {
                    Thread.sleep(100);
                    ret.incrementAndGet();
                    latch.countDown();
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
        Assert.assertTrue(executor.shutdownGracefully());
        executeShouldFail(executor);
        executeShouldFail(executor);
        Assert.assertTrue(executor.isShutdown());
        latch.await();
        Assert.assertEquals(10, ret.get());
    }

    private static void executeShouldFail(Executor executor) {
        try {
            executor.execute(() -> {
                // Noop.
            });
            Assert.fail();
        } catch (final RejectedExecutionException expected) {
            // expected
        }
    }
}
