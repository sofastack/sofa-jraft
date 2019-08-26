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

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static java.util.concurrent.Executors.newFixedThreadPool;

@ThreadSafe
public class SystemTimer implements Timer, Function<TimerTaskEntry, Void> {

    private final String                           executorName;

    private final long                             tickMs;

    private final int                              wheelSize;

    private final long                             startMs;

    private final ExecutorService                  taskExecutor;

    private DelayQueue<TimerTaskList>              delayQueue    = new DelayQueue<>();

    private final AtomicInteger                    taskCounter   = new AtomicInteger(0);

    private final TimingWheel                      timingWheel;

    /**
     * Locks used to protect data structures while ticking
     */
    private final ReentrantReadWriteLock           readWriteLock = new ReentrantReadWriteLock();

    private final ReentrantReadWriteLock.ReadLock  readLock      = readWriteLock.readLock();

    private final ReentrantReadWriteLock.WriteLock writeLock     = readWriteLock.writeLock();

    public SystemTimer(String executorName) {
        this.executorName = executorName;
        this.tickMs = 1L;
        this.wheelSize = 20;
        this.startMs = Time.SYSTEM.hiResClockMs();
        this.taskExecutor = newFixedThreadPool(1, runnable -> new Thread(runnable, "executor-" + this.executorName));
        this.timingWheel = new TimingWheel(this.tickMs, this.wheelSize, this.startMs, this.taskCounter, this.delayQueue);
    }

    /**
     * Add a new task to this executor. It will be executed after the task's delay
     * (beginning from the time of submission)
     *
     * @param timerTask the task to add
     */
    @Override
    public void add(final TimerTask timerTask) {
        this.readLock.lock();
        try {
            addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.getDelayMs() + Time.SYSTEM.hiResClockMs()));
        } finally {
            this.readLock.unlock();
        }
    }

    private void addTimerTaskEntry(TimerTaskEntry timerTaskEntry) {
        if (!this.timingWheel.add(timerTaskEntry)) {
            // Already expired or cancelled
            if (!timerTaskEntry.cancelled()) {
                this.taskExecutor.submit(timerTaskEntry.getTimerTask());
            }
        }
    }

    /**
     * Advance the internal clock, executing any tasks whose expiration has been
     * reached within the duration of the passed timeout.
     *
     * @param timeoutMs timeout millis
     * @return whether or not any tasks were executed
     */
    @Override
    public boolean advanceClock(final long timeoutMs) {
        try {
            TimerTaskList bucket = this.delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (bucket != null) {
                this.writeLock.lock();
                try {
                    while (bucket != null) {
                        this.timingWheel.advanceClock(bucket.getExpiration());
                        bucket.flush(this);
                        bucket = this.delayQueue.poll();
                    }
                } finally {
                    this.writeLock.unlock();
                }
                return true;
            } else {
                return false;
            }
        } catch (InterruptedException e) {
        }
        return false;
    }

    /**
     * Get the number of tasks pending execution
     *
     * @return the number of tasks
     */
    @Override
    public int size() {
        return this.taskCounter.get();
    }

    /**
     * Shutdown the timer service, leaving pending tasks unexecuted
     */
    @Override
    public void shutdown() {
        this.taskExecutor.shutdown();
    }

    /**
     * Applies this function to the given argument.
     *
     * @param timerTaskEntry the function argument
     * @return the function result
     */
    @Override
    public Void apply(TimerTaskEntry timerTaskEntry) {
        addTimerTaskEntry(timerTaskEntry);
        return null;
    }
}
