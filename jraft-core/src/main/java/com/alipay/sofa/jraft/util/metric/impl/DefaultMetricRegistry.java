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

import com.alipay.sofa.jraft.util.SPI;
import com.alipay.sofa.jraft.util.metric.JRaftClock;
import com.alipay.sofa.jraft.util.metric.JRaftCounter;
import com.alipay.sofa.jraft.util.metric.JRaftGauge;
import com.alipay.sofa.jraft.util.metric.JRaftHistogram;
import com.alipay.sofa.jraft.util.metric.JRaftMeter;
import com.alipay.sofa.jraft.util.metric.JRaftMetric;
import com.alipay.sofa.jraft.util.metric.JRaftMetricSet;
import com.alipay.sofa.jraft.util.metric.JRaftMetricFilter;
import com.alipay.sofa.jraft.util.metric.JRaftMetricRegistry;
import com.alipay.sofa.jraft.util.metric.JRaftTimer;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * JRaftMetricRegistry default implementation based on {@code MetricRegistry} of Dropwizard.
 *
 * @author nicholas.jxf
 */
@SPI
public class DefaultMetricRegistry implements JRaftMetricRegistry {

    private MetricRegistry metricRegistry;

    public DefaultMetricRegistry() {
        this.metricRegistry = new MetricRegistry();
    }

    @Override
    public MetricRegistry getMetricRegistry() {
        return this.metricRegistry;
    }

    @Override
    public Map<String, JRaftMetric> getMetrics() {
        final Map<String, Metric> metrics = this.metricRegistry.getMetrics();
        return metrics.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                metric -> new DefaultMetric(metric.getValue())));
    }

    @Override
    public void register(final String name, final JRaftMetricSet metricSet) {
        this.metricRegistry.register(name, metricSet(metricSet));
    }

    @Override
    public JRaftMeter meter(final String name) {
        return new DefaultMeter(this.metricRegistry.meter(name));
    }

    @Override
    public JRaftCounter counter(final String name) {
        return new DefaultCounter(this.metricRegistry.counter(name));
    }

    @Override
    public JRaftHistogram histogram(final String name) {
        return new DefaultHistogram(this.metricRegistry.histogram(name));
    }

    @Override
    public JRaftTimer timer(final String name) {
        return new DefaultTimer(this.metricRegistry.timer(name));
    }

    @Override
    public JRaftClock defaultClock() {
        return new DefaultClock(Clock.defaultClock());
    }

    @Override
    public SortedSet<String> getNames() {
        return this.metricRegistry.getNames();
    }

    @Override
    public JRaftMetricFilter getAllFilter() {
        return new DefaultMetricFilter(MetricFilter.ALL);
    }

    @Override
    public SortedMap<String, JRaftGauge> getGauges(final JRaftMetricFilter metricFilter) {
        final SortedMap<String, Gauge> gauges = this.metricRegistry.getGauges(metricFilter(metricFilter));
        return new TreeMap<>(gauges.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                gauge -> new DefaultGauge(gauge.getValue()))));
    }

    @Override
    public SortedMap<String, JRaftCounter> getCounters(final JRaftMetricFilter metricFilter) {
        final SortedMap<String, Counter> counters = this.metricRegistry.getCounters(metricFilter(metricFilter));
        return new TreeMap<>(counters.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                counter -> new DefaultCounter(counter.getValue()))));
    }

    @Override
    public SortedMap<String, JRaftHistogram> getHistograms(final JRaftMetricFilter metricFilter) {
        final SortedMap<String, Histogram> histograms = this.metricRegistry.getHistograms(metricFilter(metricFilter));
        return new TreeMap<>(histograms.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                histogram -> new DefaultHistogram(histogram.getValue()))));
    }

    @Override
    public SortedMap<String, JRaftMeter> getMeters(final JRaftMetricFilter metricFilter) {
        final SortedMap<String, Meter> meters = this.metricRegistry.getMeters(metricFilter(metricFilter));
        return new TreeMap<>(meters.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                meter -> new DefaultMeter(meter.getValue()))));
    }

    @Override
    public SortedMap<String, JRaftTimer> getTimers(final JRaftMetricFilter metricFilter) {
        final SortedMap<String, Timer> timers = this.metricRegistry.getTimers(metricFilter(metricFilter));
        return new TreeMap<>(timers.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                timer -> new DefaultTimer(timer.getValue()))));
    }

    @Override
    public boolean remove(final String name) {
        return this.metricRegistry.remove(name);
    }

    private MetricSet metricSet(final JRaftMetricSet metricSet) {
        return () -> metricSet.getMetrics().entrySet().stream()
             .collect(Collectors.toMap(Map.Entry::getKey, metric -> (Gauge)()-> (JRaftGauge)metric.getValue()));
    }

    private MetricFilter metricFilter(final JRaftMetricFilter metricFilter) {
        return (MetricFilter) metricFilter.getMetricFilter();
    }
}
