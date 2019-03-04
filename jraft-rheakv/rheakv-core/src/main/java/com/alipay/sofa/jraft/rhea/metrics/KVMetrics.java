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
package com.alipay.sofa.jraft.rhea.metrics;

import com.alipay.sofa.jraft.rhea.util.StringBuilderHelper;
import com.alipay.sofa.jraft.util.Requires;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * In rheaKV, metrics are required. As for whether to output (log) metrics results, you decide.
 *
 * @author jiachun.fjc
 */
public final class KVMetrics {

    private static final MetricRegistry metricRegistry = new MetricRegistry();

    /**
     * Return the global registry of metric instances.
     */
    public static MetricRegistry metricRegistry() {
        return metricRegistry;
    }

    /**
     * Return the {@link Meter} registered under this name; or create
     * and register a new {@link Meter} if none is registered.
     */
    public static Meter meter(final String name) {
        return metricRegistry.meter(Requires.requireNonNull(name, "name"));
    }

    /**
     * Return the {@link Meter} registered under this name; or create
     * and register a new {@link Meter} if none is registered.
     */
    public static Meter meter(final String... names) {
        return metricRegistry.meter(name(names));
    }

    /**
     * Return the {@link Timer} registered under this name; or create
     * and register a new {@link Timer} if none is registered.
     */
    public static Timer timer(final String name) {
        return metricRegistry.timer(Requires.requireNonNull(name, "name"));
    }

    /**
     * Return the {@link Timer} registered under this name; or create
     * and register a new {@link Timer} if none is registered.
     */
    public static Timer timer(final String... names) {
        return metricRegistry.timer(name(names));
    }

    /**
     * Return the {@link Counter} registered under this name; or create
     * and register a new {@link Counter} if none is registered.
     */
    public static Counter counter(final String name) {
        return metricRegistry.counter(Requires.requireNonNull(name, "name"));
    }

    /**
     * Return the {@link Counter} registered under this name; or create
     * and register a new {@link Counter} if none is registered.
     */
    public static Counter counter(final String... names) {
        return metricRegistry.counter(name(names));
    }

    /**
     * Return the {@link Histogram} registered under this name; or create
     * and register a new {@link Histogram} if none is registered.
     */
    public static Histogram histogram(final String name) {
        return metricRegistry.histogram(Requires.requireNonNull(name, "name"));
    }

    /**
     * Return the {@link Histogram} registered under this name; or create
     * and register a new {@link Histogram} if none is registered.
     */
    public static Histogram histogram(final String... names) {
        return metricRegistry.histogram(name(names));
    }

    private static String name(final String... names) {
        final StringBuilder buf = StringBuilderHelper.get();
        for (final String name : names) {
            if (buf.length() > 0) {
                buf.append('_');
            }
            buf.append(name);
        }
        return buf.toString();
    }

    private KVMetrics() {
    }
}
