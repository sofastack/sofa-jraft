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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.codahale.metrics.MetricRegistry;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author far.liu
 */
@RunWith(value = MockitoJUnitRunner.class)
public class ThreadPoolGroupTest extends TestCase {
    private static final String GROUP_ID_001   = "group001";
    private static final String GROUP_ID_002   = "group002";
    private static final String GROUP_ID_003   = "group003";
    private ThreadPoolExecutor  customExecutor = ThreadPoolUtil
                                                   .newBuilder()
                                                   .poolName("JRAFT_TEST_CUSTOM_EXECUTOR")
                                                   .enableMetric(true)
                                                   .coreThreads(Utils.MIN_CLOSURE_EXECUTOR_POOL_SIZE)
                                                   .maximumThreads(Utils.MAX_CLOSURE_EXECUTOR_POOL_SIZE)
                                                   .keepAliveSeconds(60L)
                                                   .workQueue(new SynchronousQueue<>())
                                                   .threadFactory(
                                                       new NamedThreadFactory("JRaft-Test-Custom-Executor-", true))
                                                   .build();

    @Before
    public void setup() {
        ThreadPoolGroup.registerThreadPool(new MetricRegistry(), GROUP_ID_001, null);
        ThreadPoolGroup.registerThreadPool(new MetricRegistry(), GROUP_ID_002, null);
        ThreadPoolGroup.registerThreadPool(new MetricRegistry(), GROUP_ID_003, customExecutor);
    }

    @Test
    public void testGlobalExecutor() {
        ThreadPoolExecutor executor1 = ThreadPoolGroup.getExecutor(GROUP_ID_001);
        ThreadPoolExecutor executor2 = ThreadPoolGroup.getExecutor(GROUP_ID_002);
        Assert.assertEquals(executor1, executor2);
    }

    @Test
    public void testCustomExecutor() {
        ThreadPoolExecutor executor = ThreadPoolGroup.getExecutor(GROUP_ID_003);
        Assert.assertEquals(executor, customExecutor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCustomExecutorForInvalidGroup() {
        ThreadPoolExecutor executor = ThreadPoolGroup.getExecutor("test");
    }

    @Test
    public void testRunThread() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ThreadPoolGroup.runInThread(GROUP_ID_001, () -> latch.countDown());
        latch.await();
    }

    @Test
    public void testRunClosureWithStatus() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ThreadPoolGroup.runClosureInThread(GROUP_ID_001, status -> {
            assertFalse(status.isOk());
            Assert.assertEquals(RaftError.EACCES.getNumber(), status.getCode());
            assertEquals("test 99", status.getErrorMsg());
            latch.countDown();
        }, new Status(RaftError.EACCES, "test %d", 99));
        latch.await();
    }

    @Test
    public void testRunClosure() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ThreadPoolGroup.runClosureInThread(GROUP_ID_001, status -> {
            assertTrue(status.isOk());
            latch.countDown();
        });
        latch.await();
    }
}