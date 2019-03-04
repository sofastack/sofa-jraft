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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repeatable timer based on java.util.Timer.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-30 3:45:37 PM
 */
public abstract class RepeatedTimer {

    public static final Logger LOG  = LoggerFactory.getLogger(RepeatedTimer.class);

    private final Lock         lock = new ReentrantLock();
    private final Timer        timer;
    private TimerTask          timerTask;
    private boolean            stopped;
    private volatile boolean   running;
    private boolean            destroyed;
    private boolean            invoking;
    private volatile int       timeoutMs;
    private final String       name;

    public int getTimeoutMs() {
        return this.timeoutMs;
    }

    public RepeatedTimer(String name, int timeoutMs) {
        super();
        this.name = name;
        this.timeoutMs = timeoutMs;
        this.stopped = true;
        this.timer = new Timer(this.name);
    }

    /**
     * Subclasses should implement this method for timer trigger.
     */
    protected abstract void onTrigger();

    /**
     * Adjust timeoutMs before every scheduling.
     *
     * @param timeoutMs timeout millis
     * @return timeout millis
     */
    protected int adjustTimeout(int timeoutMs) {
        return timeoutMs;
    }

    public void run() {
        lock.lock();
        try {
            this.invoking = true;
        } finally {
            lock.unlock();
        }
        try {
            onTrigger();
        } catch (Throwable t) {
            LOG.error("run timer failed", t);
        }
        boolean invokeDestroyed = false;
        lock.lock();
        try {
            this.invoking = false;
            if (this.stopped) {
                running = false;
                invokeDestroyed = destroyed;
            } else {
                this.timerTask = null;
                this.schedule();
            }
        } finally {
            lock.unlock();
        }
        if (invokeDestroyed) {
            this.onDestroy();
        }
    }

    /**
     * Run the timer at once, it will cancel the timer and re-schedule it.
     */
    public void runOnceNow() {
        lock.lock();
        try {
            if (this.timerTask != null && this.timerTask.cancel()) {
                this.timerTask = null;
                this.run();
                this.schedule();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * called after destroy timer.
     */
    protected void onDestroy() {

    }

    /**
     * Start the timer.
     */
    public void start() {
        lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            if (!this.stopped) {
                return;
            }
            this.stopped = false;
            if (this.running) {
                return;
            }
            running = true;
            schedule();
        } finally {
            lock.unlock();
        }
    }

    private void schedule() {
        if (this.timerTask != null) {
            this.timerTask.cancel();
        }
        this.timerTask = new TimerTask() {

            @Override
            public void run() {
                try {
                    RepeatedTimer.this.run();
                } catch (Throwable t) {
                    LOG.error("Run timer task failed taskName={}", name, t);
                }
            }
        };
        this.timer.schedule(this.timerTask, adjustTimeout(this.timeoutMs));
    }

    /**
     * Reset timer with new timeoutMs.
     * @param timeoutMs timeout millis
     */
    public void reset(int timeoutMs) {
        lock.lock();
        this.timeoutMs = timeoutMs;
        try {
            if (this.stopped) {
                return;
            }
            if (this.running) {
                schedule();
            }
        } finally {
            lock.lock();
        }
    }

    /**
     * reset timer with current timeoutMs
     */
    public void reset() {
        lock.lock();
        try {
            this.reset(this.timeoutMs);
        } finally {
            lock.lock();
        }
    }

    /**
     * Destroy timer
     */
    public void destroy() {
        boolean invokeDestroyed = false;
        lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            this.destroyed = true;
            if (!this.running) {
                invokeDestroyed = true;
            }
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timerTask != null) {
                if (this.timerTask.cancel()) {
                    invokeDestroyed = true;
                    this.running = false;
                }
                this.timerTask = null;
            }
            this.timer.cancel();
        } finally {
            lock.unlock();
            if (invokeDestroyed) {
                onDestroy();
            }
        }
    }

    @Override
    public String toString() {
        return "RepeatedTimer [lock=" + this.lock + ", timerTask=" + this.timerTask + ", stopped=" + this.stopped
               + ", running=" + this.running + ", destroyed=" + this.destroyed + ", invoking=" + this.invoking
               + ", timeoutMs=" + this.timeoutMs + "]";
    }

    /**
     * Stop timer
     */
    public void stop() {
        lock.lock();
        try {
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timerTask != null) {
                this.timerTask.cancel();
                this.running = false;
                this.timerTask = null;
            }
        } finally {
            lock.unlock();
        }
    }

}
