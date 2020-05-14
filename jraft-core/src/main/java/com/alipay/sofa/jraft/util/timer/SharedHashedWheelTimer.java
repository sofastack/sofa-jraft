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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Requires;

/**
 * @author zongtanghu
 */
public class SharedHashedWheelTimer implements Timer {

    private final Timer[] hashedWheelTimers;
    private final int     workerNum;

    public SharedHashedWheelTimer(int workerNum, String name) {
        this.hashedWheelTimers = new HashedWheelTimer[workerNum];
        this.workerNum = workerNum;

        for (int i = 0; i < workerNum; i++) {
            String workName = i + "-" + name;
            this.hashedWheelTimers[i] = createSingleHashedWheelTimer(workName);
        }
    }

    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        Requires.requireNonNull(task, "task");
        Requires.requireNonNull(unit, "unit");
        int worker = randomTimerWorker(this.workerNum);
        return this.hashedWheelTimers[worker].newTimeout(task, delay, unit);
    }

    @Override
    public Set<Timeout> stop() {
        Set<Timeout> ret = new HashSet<Timeout>();
        for (int i = 0; i < this.workerNum; i++) {
            ret.addAll(this.hashedWheelTimers[i].stop());
        }
        return ret;
    }

    private static Timer createSingleHashedWheelTimer(final String name) {
        return new HashedWheelTimer(new NamedThreadFactory(name, true), 1, TimeUnit.MILLISECONDS, 2048);
    }

    private int randomTimerWorker(int workerNum) {
        return ThreadLocalRandom.current().nextInt(0, workerNum);
    }
}
