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
package com.alipay.sofa.jraft.example.rheakv;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;

/**
 *
 * @author jiachun.fjc
 */
public class DistributedLockExample {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedLockExample.class);

    public static void main(final String[] args) throws Exception {
        final Client client = new Client();
        client.init();
        lock(client.getRheaKVStore());
        lockAndAutoKeepLease(client.getRheaKVStore());
        client.shutdown();
    }

    public static void lock(final RheaKVStore rheaKVStore) {
        final String lockKey = "lock_example";
        final DistributedLock<byte[]> lock = rheaKVStore.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
        if (lock.tryLock()) {
            try {
                LOG.info("Lock success with: {}", lockKey);
            } finally {
                lock.unlock();
            }
        } else {
            LOG.info("Fail to lock with: {}", lockKey);
        }
    }

    public static void lockAndAutoKeepLease(final RheaKVStore rheaKVStore) {
        final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor();
        final String lockKey = "lock_example1";
        final DistributedLock<byte[]> lock = rheaKVStore.getDistributedLock(lockKey, 3, TimeUnit.SECONDS, watchdog);
        if (lock.tryLock()) {
            try {
                LOG.info("Lock success with: {}", lockKey);
            } finally {
                lock.unlock();
            }
        } else {
            LOG.info("Fail to lock with: {}", lockKey);
        }
        ExecutorServiceHelper.shutdownAndAwaitTermination(watchdog);
    }
}
