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

import com.alipay.sofa.jraft.util.metric.JRaftCounter;

import com.codahale.metrics.Counter;

/**
 * JRaftCounter default implementation based on {@code Counter} of Dropwizard.
 *
 * @author nicholas.jxf
 */
public class DefaultCounter implements JRaftCounter {

    private Counter counter;

    DefaultCounter(final Counter counter) {
        this.counter = counter;
    }

    @Override
    public long getCount() {
        return this.counter.getCount();
    }

    @Override
    public void inc() {
        this.counter.inc();
    }

    @Override
    public void inc(final long num) {
        this.counter.inc(num);
    }

    @Override
    public void dec(final long n) {
        this.counter.dec(n);
    }
}
