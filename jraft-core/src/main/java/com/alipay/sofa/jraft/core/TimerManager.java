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
package com.alipay.sofa.jraft.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.util.NamedThreadFactory;

/**
 * The global timer manager.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-30 3:24:34 PM
 */
public class TimerManager implements Lifecycle<Integer> {

    private ScheduledExecutorService executor;

    @Override
    public boolean init(Integer coreSize) {
        executor = Executors.newScheduledThreadPool(coreSize, new NamedThreadFactory("JRaft-Node-ScheduleThreadPool-"));
        return true;
    }

    @Override
    public void shutdown() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    private void checkStarted() {
        if (executor == null) {
            throw new IllegalStateException("Please init timer manager.");
        }
    }

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        checkStarted();
        return executor.schedule(command, delay, unit);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        checkStarted();
        return executor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        checkStarted();
        return executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
}
