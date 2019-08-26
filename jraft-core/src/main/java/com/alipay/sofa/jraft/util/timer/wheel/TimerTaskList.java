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
package com.alipay.sofa.jraft.util.timer.wheel;

import com.alipay.sofa.jraft.util.Time;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class TimerTaskList implements Delayed {

    private final AtomicInteger  taskCounter;

    private final TimerTaskEntry root;

    private final AtomicLong     expiration;

    public TimerTaskList(AtomicInteger taskCounter) {
        this.taskCounter = taskCounter;
        // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
        // root.next points to the head
        // root.prev points to the tail
        this.root = new TimerTaskEntry(null, -1L);
        this.root.next = this.root;
        this.root.prev = this.root;
        this.expiration = new AtomicLong(-1L);
    }

    /**
     * Set the bucket's expiration time.
     * Returns true if the expiration time is changed
     * @param expirationMs expiration time
     * @return result of setting expiration time
     */
    public boolean setExpiration(final Long expirationMs) {
        return this.expiration.getAndSet(expirationMs) != expirationMs;
    }

    /**
     * Get the bucket's expiration time.
     * @return expiration time
     */
    public Long getExpiration() {
        return this.expiration.get();
    }

    /**
     * Apply the supplied function to each of tasks in this list
     * @param f the supplied function
     */
    public void foreach(final Function<TimerTask, Void> f) {
        synchronized (this) {
            TimerTaskEntry entry = this.root.next;
            while (entry != this.root) {
                final TimerTaskEntry nextEntry = entry.next;

                if (!entry.cancelled()) {
                    f.apply(entry.getTimerTask());
                }

                entry = nextEntry;
            }
        }
    }

    /**
     * Add a timer task entry to this list
     * @param timerTaskEntry time task entity
     */
    public void add(final TimerTaskEntry timerTaskEntry) {
        boolean done = false;
        while (!done) {
            // Remove the timer task entry if it is already in any other list
            // We do this outside of the sync block below to avoid deadlocking.
            // We may retry until timerTaskEntry.list becomes null.
            timerTaskEntry.remove();

            synchronized (this) {
                synchronized (timerTaskEntry) {
                    if (timerTaskEntry.getList() == null) {
                        // put the timer task entry to the end of the list. (root.prev points to the tail entry)
                        TimerTaskEntry tail = this.root.prev;
                        timerTaskEntry.next = this.root;
                        timerTaskEntry.prev = tail;
                        timerTaskEntry.setList(this);
                        tail.next = timerTaskEntry;
                        this.root.prev = timerTaskEntry;
                        this.taskCounter.incrementAndGet();
                        done = true;
                    }
                }
            }
        }
    }

    /**
     * Remove the specified timer task entry from this list
     * @param timerTaskEntry time task entity
     */
    public void remove(final TimerTaskEntry timerTaskEntry) {
        synchronized (this) {
            synchronized (timerTaskEntry) {
                if (timerTaskEntry.getList() == this) {
                    timerTaskEntry.next.prev = timerTaskEntry.prev;
                    timerTaskEntry.prev.next = timerTaskEntry.next;
                    timerTaskEntry.next = null;
                    timerTaskEntry.prev = null;
                    timerTaskEntry.setList(null);
                    this.taskCounter.decrementAndGet();
                }
            }
        }
    }

    /**
     * Remove all task entries and apply the supplied function to each of them
     * @param f the supplied function
     */
    public void flush(final Function<TimerTaskEntry, Void> f) {
        synchronized (this) {
            TimerTaskEntry head = this.root.next;
            while (head != this.root) {
                remove(head);
                f.apply(head);
                head = this.root.next;
            }
            this.expiration.set(-1L);
        }
    }

    @Override
    public long getDelay(final TimeUnit unit) {
        return unit.convert(Long.max(getExpiration() - Time.SYSTEM.hiResClockMs(), 0), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(final Delayed d) {
        TimerTaskList other;
        if (d instanceof TimerTaskList) {
            other = (TimerTaskList) d;
        } else {
            throw new ClassCastException("can not cast to TimerTaskList");
        }

        return getExpiration().compareTo(other.getExpiration());
    }
}
