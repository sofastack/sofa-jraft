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

import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class TimingWheel {

    private final long                      tickMs;

    private final int                       wheelSize;

    private final long                      startMs;

    private final AtomicInteger             taskCounter;

    private final DelayQueue<TimerTaskList> queue;

    private final long                      interval;

    private final TimerTaskList[]           buckets;

    private long                            currentTime;

    private volatile TimingWheel            overflowWheel;

    public TimingWheel(long tickMs, int wheelSize, long startMs, AtomicInteger taskCounter,
                       DelayQueue<TimerTaskList> queue) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.startMs = startMs;
        this.taskCounter = taskCounter;
        this.queue = queue;
        this.interval = tickMs * wheelSize;
        this.currentTime = this.startMs - (this.startMs % tickMs);
        this.buckets = new TimerTaskList[wheelSize];
        for (int i = 0; i < this.buckets.length; i++) {
            this.buckets[i] = new TimerTaskList(taskCounter);
        }
    }

    private void addOverflowWheel() {
        synchronized (this) {
            if (this.overflowWheel == null) {
                this.overflowWheel = new TimingWheel(this.interval, this.wheelSize, this.currentTime, this.taskCounter,
                    this.queue);
            }
        }
    }

    public Boolean add(final TimerTaskEntry timerTaskEntry) {
        long expiration = timerTaskEntry.getExpirationMs();

        if (timerTaskEntry.cancelled()) {
            // Cancelled
            return false;
        } else if (expiration < this.currentTime + this.tickMs) {
            // Already expired
            return false;
        } else if (expiration < this.currentTime + this.interval) {
            // Put in its own bucket
            final long virtualId = expiration / this.tickMs;
            final TimerTaskList bucket = this.buckets[(int) (virtualId % this.wheelSize)];
            bucket.add(timerTaskEntry);

            // Set the bucket expiration time
            if (bucket.setExpiration(virtualId * this.tickMs)) {
                // The bucket needs to be enqueued because it was an expired bucket
                // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
                // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
                // will pass in the same value and hence return false, thus the bucket with the same expiration will not
                // be enqueued multiple times.
                this.queue.offer(bucket);
            }
            return true;
        } else {
            // Out of the interval. Put it into the parent timer
            if (this.overflowWheel == null) {
                addOverflowWheel();
            }
            return this.overflowWheel.add(timerTaskEntry);
        }
    }

    public void advanceClock(final long timeMs) {
        if (timeMs >= this.currentTime + this.tickMs) {
            this.currentTime = timeMs - (timeMs % this.tickMs);

            // Try to advance the clock of the overflow wheel if present
            if (this.overflowWheel != null) {
                this.overflowWheel.advanceClock(this.currentTime);
            }
        }
    }
}
