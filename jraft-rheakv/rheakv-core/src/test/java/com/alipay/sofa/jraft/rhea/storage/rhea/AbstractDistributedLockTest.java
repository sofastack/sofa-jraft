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
package com.alipay.sofa.jraft.rhea.storage.rhea;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.errors.InvalidLockAcquirerException;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author jiachun.fjc
 */
public abstract class AbstractDistributedLockTest extends RheaKVTestCluster {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    @Rule
    public TestName                      testName = new TestName();

    public abstract StorageType getStorageType();

    @Before
    public void setup() throws Exception {
        System.out.println(">>>>>>>>>>>>>>> Start test method: " + this.testName.getMethodName());
        super.start(getStorageType());
    }

    @After
    public void tearDown() throws Exception {
        super.shutdown();
        System.out.println(">>>>>>>>>>>>>>> End test method: " + this.testName.getMethodName());
    }

    private void lockTest(final RheaKVStore store) throws Exception {
        final String lockKey = "lockTest";
        final DistributedLock<byte[]> lock = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
        assertNotNull(lock);
        boolean locked = lock.tryLock();
        assertTrue(locked);
        Future<Boolean> f = EXECUTOR.submit(() -> store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS).tryLock());
        Assert.assertTrue(!f.get());
        lock.unlock();
        f = EXECUTOR.submit(() -> store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS).tryLock());
        Assert.assertTrue(f.get());
    }

    private void lockKeepLeaseTest(final RheaKVStore store) throws Exception {
        final String lockKey = "lockKeepLeaseTest";
        final ScheduledExecutorService watchdog = Executors.newScheduledThreadPool(2);
        final DistributedLock<byte[]> lock = store.getDistributedLock(lockKey, 1, TimeUnit.SECONDS, watchdog);
        assertNotNull(lock);
        boolean locked = lock.tryLock();
        assertTrue(locked);
        Thread.sleep(2000);
        Future<Boolean> f = EXECUTOR.submit(() -> store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS).tryLock());
        Assert.assertTrue(!f.get());
        lock.unlock();
        f = EXECUTOR.submit(() -> store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS).tryLock());
        Assert.assertTrue(f.get());
        ExecutorServiceHelper.shutdownAndAwaitTermination(watchdog);
    }

    private void reentrantLockTest(final RheaKVStore store) throws Exception {
        final String lockKey = "reentrantLockTest";
        final DistributedLock<byte[]> lock = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
        assertNotNull(lock);
        for (int i = 0; i < 5; i++) {
            boolean locked = lock.tryLock();
            assertTrue(locked);
            assertEquals(i + 1L, lock.getOwner().getAcquires());
        }
        for (int i = 5; i > 0; i--) {
            lock.unlock();
        }
        lock.tryLock();
        assertEquals(1L, lock.getOwner().getAcquires());
    }

    private void lockFencingTest(final RheaKVStore store) throws Exception {
        final String lockKey = "lockFencingTest";
        final DistributedLock<byte[]> lock = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
        assertNotNull(lock);
        long prevToken = 0;
        for (int i = 0; i < 5; i++) {
            boolean locked = lock.tryLock();
            assertTrue(locked);
            final long token = lock.getFencingToken();
            System.err.println("token=" + token);
            assertTrue(token > prevToken);
            prevToken = token;
            lock.unlock();
        }
    }

    private void invalidUnlockTest(final RheaKVStore store) throws Exception {
        final String lockKey = "invalidUnlock";
        final DistributedLock<byte[]> lock = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
        assertNotNull(lock);
        assertTrue(lock.tryLock());
        final DistributedLock<byte[]> anotherLock = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
        try {
            anotherLock.unlock();
            assertTrue(false);
        } catch (final Exception e) {
            e.printStackTrace();
            assertTrue(e instanceof InvalidLockAcquirerException);
        }
    }

    @SuppressWarnings("unchecked")
    private void concurrentLockingTest(final RheaKVStore store) throws Exception {
        final String lockKey = "concurrentLockingTest";
        Future<Boolean> f1 = EXECUTOR.submit(() -> store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS).tryLock());
        Future<Boolean> f2 = EXECUTOR.submit(() -> store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS).tryLock());
        Future<Boolean> f3 = EXECUTOR.submit(() -> store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS).tryLock());
        Future<Boolean> f4 = EXECUTOR.submit(() -> store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS).tryLock());
        assertTrue(f1.get() || f2.get() || f3.get() || f4.get());
        assertTrue(!(f1.get() && f2.get() && f3.get() && f4.get()));
        int count = 0;
        for (Future<Boolean> f : new Future[] { f1, f2, f3, f4}) {
            if (f.get()) {
                count++;
            }
        }
        assertEquals(1, count);
    }

    private void lockCtxTest(final RheaKVStore store) throws Exception {
        final String lockKey = "lockCtxTest";
        final DistributedLock<byte[]> lock = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
        assertNotNull(lock);
        final byte[] ctx1 = BytesUtil.writeUtf8("first");
        assertTrue(lock.tryLock(ctx1));
        assertArrayEquals(ctx1, lock.getOwnerContext());

        final DistributedLock<byte[]> anotherLock = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
        assertNotNull(anotherLock);
        final byte[] ctx2 = BytesUtil.writeUtf8("second");
        assertTrue(!anotherLock.tryLock(ctx2));
        assertArrayEquals(ctx1, anotherLock.getOwnerContext());
        assertEquals(lock.getAcquirer().getId(), anotherLock.getOwner().getId());
    }

    @Test
    public void lockByLeaderTest() throws Exception {
        lockTest(getRandomLeaderStore());
    }

    @Test
    public void lockByFollowerTest() throws Exception {
        lockTest(getRandomFollowerStore());
    }

    @Test
    public void lockKeepLeaseByLeaderTest() throws Exception {
        lockKeepLeaseTest(getRandomLeaderStore());
    }

    @Test
    public void lockKeepLeaseByFollowerTest() throws Exception {
        lockKeepLeaseTest(getRandomFollowerStore());
    }

    @Test
    public void reentrantLockByLeaderTest() throws Exception {
        reentrantLockTest(getRandomLeaderStore());
    }

    @Test
    public void reentrantLockByFollowerTest() throws Exception {
        reentrantLockTest(getRandomFollowerStore());
    }

    @Test
    public void lockFencingByLeaderTest() throws Exception {
        lockFencingTest(getRandomLeaderStore());
    }

    @Test
    public void lockFencingByFollowerTest() throws Exception {
        lockFencingTest(getRandomFollowerStore());
    }

    @Test
    public void invalidUnlockByLeaderTest() throws Exception {
        invalidUnlockTest(getRandomLeaderStore());
    }

    @Test
    public void invalidUnlockByFollowerTest() throws Exception {
        invalidUnlockTest(getRandomFollowerStore());
    }

    @Test
    public void concurrentLockingByLeaderTest() throws Exception {
        concurrentLockingTest(getRandomLeaderStore());
    }

    @Test
    public void concurrentLockingByFollowerTest() throws Exception {
        concurrentLockingTest(getRandomFollowerStore());
    }

    @Test
    public void lockCtxByLeaderTest() throws Exception {
        lockCtxTest(getRandomLeaderStore());
    }

    @Test
    public void lockCtxByFollowerTest() throws Exception {
        lockCtxTest(getRandomFollowerStore());
    }
}
