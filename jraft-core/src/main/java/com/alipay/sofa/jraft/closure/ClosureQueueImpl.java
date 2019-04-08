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
        return firstIndex;
    }

    @OnlyForTest
    public LinkedList<Closure> getQueue() {
        return queue;
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
        this.lock.lock();
        try {
            this.firstIndex = 0;
            savedQueue = this.queue;
            this.queue = new LinkedList<>();
        } finally {
            this.lock.unlock();
        }

        final Status status = new Status(RaftError.EPERM, "Leader stepped down");
        for (final Closure done : savedQueue) {
            if (done != null) {
                Utils.runClosureInThread(done, status);
            }
        }
    }

    @Override
    public void resetFirstIndex(final long firstIndex) {
        this.lock.lock();
        try {
            Requires.requireTrue(this.queue.isEmpty(), "Queue is not empty.");
            this.firstIndex = firstIndex;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void appendPendingClosure(final Closure closure) {
        this.lock.lock();
        try {
            this.queue.add(closure);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public Popped popClosureUntil(final long endIndex, final List<Closure> out) {
        out.clear();
        this.lock.lock();
        try {
            final int queueSize = this.queue.size();
            if (queueSize == 0 || endIndex < this.firstIndex) {
                return Popped.of(endIndex + 1, false);
            }
            if (endIndex > this.firstIndex + queueSize - 1) {
                LOG.error("Invalid endIndex={}, firstIndex={}, closureQueueSize={}", endIndex, this.firstIndex,
                    queueSize);
                return Popped.of(-1, false);
            }
            final long outFirstIndex = this.firstIndex;
            boolean hasTaskClosure = false;
            for (long i = outFirstIndex; i <= endIndex; i++) {
                final Closure closure = this.queue.pollFirst();
                hasTaskClosure |= closure instanceof TaskClosure;
                out.add(closure);
            }
            this.firstIndex = endIndex + 1;
            return Popped.of(outFirstIndex, hasTaskClosure);
        } finally {
            this.lock.unlock();
        }
    }
}
