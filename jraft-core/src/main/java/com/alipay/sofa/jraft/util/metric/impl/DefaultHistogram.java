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

import com.alipay.sofa.jraft.util.metric.JRaftHistogram;
import com.alipay.sofa.jraft.util.metric.JRaftSnapshot;

import com.codahale.metrics.Histogram;

/**
 * JRaftHistogram default implementation based on {@code Histogram} of Dropwizard.
 *
 * @author nicholas.jxf
 */
public class DefaultHistogram implements JRaftHistogram {

    private Histogram histogram;

    DefaultHistogram(final Histogram histogram) {
        this.histogram = histogram;
    }

    @Override
    public long getCount() {
        return this.histogram.getCount();
    }

    @Override
    public JRaftSnapshot getSnapshot() {
        return new DefaultSnapshot(this.histogram.getSnapshot());
    }

    @Override
    public void update(final long value) {
        this.histogram.update(value);
    }
}
