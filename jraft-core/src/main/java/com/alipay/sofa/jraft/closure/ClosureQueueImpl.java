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
package com.alipay.sofa.jraft.closure;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Closure queue implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-28 11:44:01 AM
 */
public class ClosureQueueImpl implements ClosureQueue {

    private static final Logger LOG = LoggerFactory.getLogger(ClosureQueueImpl.class);

    private final Lock          lock;
    private long                firstIndex;
    private LinkedList<Closure> queue;

    @OnlyForTest
    public long getFirstIndex() {
        return this.firstIndex;
    }

    @OnlyForTest
    public LinkedList<Closure> getQueue() {
        return this.queue;
    }

    public ClosureQueueImpl() {
        super();
        this.lock = new ReentrantLock();
        this.firstIndex = 0;
        this.queue = new LinkedList<>();
    }

    @Override
    public void clear() {
        List<Closure> savedQueue;
        lock.lock();
        try {
            this.firstIndex = 0;
            savedQueue = this.queue;
            this.queue = new LinkedList<>();
        } finally {
            lock.unlock();
        }

        Status status = new Status(RaftError.EPERM, "Leader stepped down");
        for (Closure done : savedQueue) {
            if (done != null) {
                Utils.runClosureInThread(done, status);
            }
        }
    }

    @Override
    public void resetFirstIndex(long firstIndex) {
        lock.lock();
        try {
            Requires.requireTrue(this.queue.isEmpty(), "Queue is not empty.");
            this.firstIndex = firstIndex;
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void appendPendingClosure(Closure closure) {
        lock.lock();
        try {
            this.queue.add(closure);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long popClosureUntil(long index, List<Closure> out) {
        out.clear();
        long outFirstIndex;
        lock.lock();
        try {
            if (queue.isEmpty() || index < this.firstIndex) {
                outFirstIndex = index + 1;
                return outFirstIndex;
            }
            if (index > this.firstIndex + queue.size() - 1) {
                LOG.error("Invalid index={}, firstIndex={}, closureQueueSize={}", index, firstIndex, queue.size());
                return -1;
            }
            outFirstIndex = this.firstIndex;
            for (long i = this.firstIndex; i <= index; i++) {
                out.add(queue.pollFirst());
            }
            this.firstIndex = index + 1;
            return outFirstIndex;
        } finally {
            lock.unlock();
        }
    }

}
