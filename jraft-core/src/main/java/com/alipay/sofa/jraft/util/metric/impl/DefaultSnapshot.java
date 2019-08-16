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

import com.codahale.metrics.Snapshot;

/**
 * JRaftSnapshot default implementation based on {@code Snapshot} of Dropwizard.
 *
 * @author nicholas.jxf
 */
public class DefaultSnapshot implements JRaftSnapshot {

    private Snapshot snapshot;

    DefaultSnapshot(final Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public double getMedian() {
        return this.snapshot.getMedian();
    }

    @Override
    public double get75thPercentile() {
        return this.snapshot.get75thPercentile();
    }

    @Override
    public double get95thPercentile() {
        return this.snapshot.get95thPercentile();
    }

    @Override
    public double get98thPercentile() {
        return this.snapshot.get98thPercentile();
    }

    @Override
    public double get99thPercentile() {
        return this.snapshot.get99thPercentile();
    }

    @Override
    public double get999thPercentile() {
        return this.snapshot.get999thPercentile();
    }

    @Override
    public long getMax() {
        return this.snapshot.getMax();
    }

    @Override
    public double getMean() {
        return this.snapshot.getMean();
    }

    @Override
    public long getMin() {
        return this.snapshot.getMin();
    }

    @Override
    public double getStdDev() {
        return this.snapshot.getStdDev();
    }
}
