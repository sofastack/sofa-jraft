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
package com.alipay.sofa.jraft.util.metric.impl;

import com.alipay.sofa.jraft.util.metric.JRaftSnapshot;
import com.alipay.sofa.jraft.util.metric.JRaftTimer;

import com.codahale.metrics.Timer;

import java.util.concurrent.TimeUnit;

/**
 * JRaftTimer default implementation based on {@code Timer} of Dropwizard.
 *
 * @author nicholas.jxf
 */
public class DefaultTimer implements JRaftTimer {

    private Timer timer;

    DefaultTimer(final Timer timer) {
        this.timer = timer;
    }

    @Override
    public Context time() {
        return new DefaultContext(this.timer.time());
    }

    @Override
    public long getCount() {
        return this.timer.getCount();
    }

    @Override
    public double getFifteenMinuteRate() {
        return this.timer.getFifteenMinuteRate();
    }

    @Override
    public double getFiveMinuteRate() {
        return this.timer.getFiveMinuteRate();
    }

    @Override
    public double getMeanRate() {
        return this.timer.getMeanRate();
    }

    @Override
    public double getOneMinuteRate() {
        return this.timer.getOneMinuteRate();
    }

    @Override
    public JRaftSnapshot getSnapshot() {
        return new DefaultSnapshot(this.timer.getSnapshot());
    }

    @Override
    public void update(final long duration, final TimeUnit unit) {
        this.timer.update(duration, unit);
    }

    public class DefaultContext implements Context {

        private Timer.Context context;

        DefaultContext(final Timer.Context context) {
            this.context = context;
        }

        public long stop() {
            return this.context.stop();
        }

        public void close() {
            this.context.close();
        }
    }
}
