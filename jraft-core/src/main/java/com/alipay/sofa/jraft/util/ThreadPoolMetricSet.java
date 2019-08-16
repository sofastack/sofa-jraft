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

import com.alipay.sofa.jraft.util.metric.JRaftGauge;
import com.alipay.sofa.jraft.util.metric.JRaftMetric;
import com.alipay.sofa.jraft.util.metric.JRaftMetricSet;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Thread pool metric set including pool-size, queued, active, completed etc.
 */
public final class ThreadPoolMetricSet implements JRaftMetricSet {

    private final ThreadPoolExecutor executor;

    public ThreadPoolMetricSet(ThreadPoolExecutor rpcExecutor) {
        super();
        this.executor = rpcExecutor;
    }

    /**
     * Return thread pool metrics
     * @return thread pool metrics map
     */
    @Override
    public Map<String, JRaftMetric> getMetrics() {
        final Map<String, JRaftMetric> gauges = new HashMap<>();
        gauges.put("pool-size", (JRaftGauge<Integer>) this.executor::getPoolSize);
        gauges.put("queued", (JRaftGauge<Integer>) this.executor.getQueue()::size);
        gauges.put("active", (JRaftGauge<Integer>) this.executor::getActiveCount);
        gauges.put("completed", (JRaftGauge<Long>) this.executor::getCompletedTaskCount);
        return gauges;
    }
}
