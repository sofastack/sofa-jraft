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

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * Fork from jdk#ArrayBlockingQueue
 */
public class ArrayBlockingQueue2<E> implements BlockingQueue2<E> {

    final Object[]          items;

    /** items index for next take, poll, peek or remove */
    int                     takeIndex;

    /** items index for next put, offer, or add */
    int                     putIndex;

    /** Number of elements in the queue */
    int                     count;

    /*
     * Concurrency control uses the classic two-condition algorithm
     * found in any textbook.
     */
    /** Main lock guarding all access */
    final ReentrantLock     lock;

    /** Condition for waiting takes */
    private final Condition notEmpty;

    /** Condition for waiting puts */
    private final Condition notFull;

    public ArrayBlockingQueue2(final int capacity) {
        this.items = new Object[capacity];
        lock = new ReentrantLock(false);
        notEmpty = lock.newCondition();
        notFull = lock.newCondition();
    }

    public void offer(E e, int timeout, TimeUnit unit) throws InterruptedException {
        checkNotNull(e);
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (count == items.length) {
                if (nanos <= 0)
                    return;
                nanos = notEmpty.awaitNanos(nanos);
            }
            enqueue(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Inserts element at current put position, advances, and signals.
     * Call only when holding lock.
     */
    private void enqueue(E x) {
        final Object[] items = this.items;
        items[putIndex] = x;
        if (++putIndex == items.length)
            putIndex = 0;
        count++;
        notEmpty.signal();
    }

    public int blockingDrainTo(Collection<? super E> c, int maxElements, int timeout, TimeUnit unit)
                                                                                                    throws InterruptedException {
        checkNotNull(c);
        if (maxElements <= 0)
            return 0;
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            while (count == 0) {
                if (nanos <= 0)
                    return 0;
                nanos = notEmpty.awaitNanos(nanos);
            }
            final Object[] items = this.items;
            int n = Math.min(maxElements, count);
            int take = takeIndex;
            int i = 0;
            try {
                while (i < n) {
                    @SuppressWarnings("unchecked")
                    E x = (E) items[take];
                    c.add(x);
                    items[take] = null;
                    if (++take == items.length)
                        take = 0;
                    i++;
                }
                return n;
            } finally {
                // Restore invariants even if c.add() threw
                if (i > 0) {
                    count -= i;
                    takeIndex = take;
                    for (; i > 0 && lock.hasWaiters(notFull); i--)
                        notFull.signal();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private static void checkNotNull(Object v) {
        if (v == null)
            throw new NullPointerException();
    }

}
