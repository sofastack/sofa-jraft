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

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hzh (642256541@qq.com)
 */
public abstract class ShutdownAbleThread implements Runnable {
    private static final Logger LOG       = LoggerFactory.getLogger(ShutdownAbleThread.class);

    private static final long   JOIN_TIME = 90 * 1000;

    private Thread              thread;
    protected volatile boolean  stopped   = false;
    private final AtomicBoolean started   = new AtomicBoolean(false);

    public ShutdownAbleThread() {
    }

    public abstract void onShutdown();

    public String getServiceName() {
        return getClass().getSimpleName();
    }

    public void start() {
        LOG.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), this.started.get(),
            this.thread);
        if (!this.started.compareAndSet(false, true)) {
            return;
        }
        this.stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        LOG.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), this.started.get(),
            this.thread);
        if (!this.started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        try {
            if (interrupt) {
                this.thread.interrupt();
            }
            this.thread.join(JOIN_TIME);
        } catch (final InterruptedException e) {
            LOG.error("Error when shutdown thread, serviceName:{}", getServiceName(), e);
        }
    }

    protected void waitForRunning(long interval) throws InterruptedException {
        Thread.sleep(interval);
    }

    public boolean isStopped() {
        return this.stopped;
    }

}
