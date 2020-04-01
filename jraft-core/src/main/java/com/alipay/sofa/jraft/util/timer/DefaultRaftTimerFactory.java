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
package com.alipay.sofa.jraft.util.timer;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.SPI;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.alipay.sofa.jraft.util.Utils;

/**
 *
 * @author jiachun.fjc
 */
@SPI
public class DefaultRaftTimerFactory implements RaftTimerFactory {

    private static final String             GLOBAL_ELECTION_TIMER_WORKERS  = "jraft.timer.global_election_timer_workers";
    private static final String             GLOBAL_VOTE_TIMER_WORKERS      = "jraft.timer.global_vote_timer_workers";
    private static final String             GLOBAL_STEP_DOWN_TIMER_WORKERS = "jraft.timer.global_step_down_timer_workers";
    private static final String             GLOBAL_SNAPSHOT_TIMER_WORKERS  = "jraft.timer.global_snapshot_timer_workers";

    private static final SharedTimerManager ELECTION_TIMER_MANAGER         = new SharedTimerManager(
                                                                               SystemPropertyUtil.getInt(
                                                                                   GLOBAL_ELECTION_TIMER_WORKERS,
                                                                                   Utils.cpus()),
                                                                               "JRaft-Global-ElectionTimer");
    private static final SharedTimerManager VOTE_TIMER_MANAGER             = new SharedTimerManager(
                                                                               SystemPropertyUtil.getInt(
                                                                                   GLOBAL_VOTE_TIMER_WORKERS,
                                                                                   Utils.cpus()),
                                                                               "JRaft-Global-VoteTimer");
    private static final SharedTimerManager STEP_DOWN_TIMER_MANAGER        = new SharedTimerManager(
                                                                               SystemPropertyUtil.getInt(
                                                                                   GLOBAL_STEP_DOWN_TIMER_WORKERS,
                                                                                   Utils.cpus()),
                                                                               "JRaft-Global-StepDownTimer");
    private static final SharedTimerManager SNAPSHOT_TIMER_MANAGER         = new SharedTimerManager(
                                                                               SystemPropertyUtil.getInt(
                                                                                   GLOBAL_SNAPSHOT_TIMER_WORKERS,
                                                                                   Utils.cpus()),
                                                                               "JRaft-Global-SnapshotTimer");

    @Override
    public Timer getElectionTimer(final boolean shared, final String name) {
        return shared ? ELECTION_TIMER_MANAGER.getRef() : createTimer(name);
    }

    @Override
    public Timer getVoteTimer(final boolean shared, final String name) {
        return shared ? VOTE_TIMER_MANAGER.getRef() : createTimer(name);
    }

    @Override
    public Timer getStepDownTimer(final boolean shared, final String name) {
        return shared ? STEP_DOWN_TIMER_MANAGER.getRef() : createTimer(name);
    }

    @Override
    public Timer getSnapshotTimer(final boolean shared, final String name) {
        return shared ? SNAPSHOT_TIMER_MANAGER.getRef() : createTimer(name);
    }

    private static Timer createTimer(final String name) {
        return new HashedWheelTimer(new NamedThreadFactory(name, true), 1, TimeUnit.MILLISECONDS, 2048);
    }

    private static class SharedTimerManager {

        private final int    workNum;
        private final String name;
        private SharedTimer  sharedTimer;

        public SharedTimerManager(int workNum, String name) {
            this.workNum = workNum;
            this.name = name;
        }

        private synchronized SharedTimer getRef() {
            if (this.sharedTimer == null || this.sharedTimer.isStopped()) {
                this.sharedTimer = new SharedTimer(new DefaultTimer(this.workNum, this.name));
            }
            return this.sharedTimer.getRef();
        }
    }

    private static class SharedTimer implements Timer {

        private AtomicInteger refCount = new AtomicInteger(0);
        private AtomicBoolean started  = new AtomicBoolean(true);
        private final Timer   timer;

        private SharedTimer(Timer timer) {
            this.timer = timer;
        }

        public SharedTimer getRef() {
            if (this.started.get()) {
                this.refCount.incrementAndGet();
                return this;
            }
            throw new IllegalStateException("Shared timer stopped");
        }

        public boolean isStopped() {
            return !this.started.get();
        }

        @Override
        public Timeout newTimeout(final TimerTask task, final long delay, final TimeUnit unit) {
            return this.timer.newTimeout(task, delay, unit);
        }

        @Override
        public Set<Timeout> stop() {
            if (this.refCount.decrementAndGet() <= 0 && this.started.compareAndSet(true, false)) {
                return this.timer.stop();
            }
            return Collections.emptySet();
        }
    }
}
