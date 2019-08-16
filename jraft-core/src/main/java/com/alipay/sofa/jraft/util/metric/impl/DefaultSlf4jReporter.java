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
import com.alipay.sofa.jraft.util.metric.JRaftMetricRegistry;
import com.alipay.sofa.jraft.util.metric.JRaftSlf4jReporter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;

import org.slf4j.Logger;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * JRaftSlf4jReporter default implementation based on {@code Slf4jReporter} of Dropwizard.
 *
 * @author nicholas.jxf
 */
@SPI
public class DefaultSlf4jReporter implements JRaftSlf4jReporter {

    private Slf4jReporter slf4jReporter;

    DefaultSlf4jReporter(final Slf4jReporter slf4jReporter) {
        this.slf4jReporter = slf4jReporter;
    }

    @Override
    public Builder forRegistry(final JRaftMetricRegistry registry) {
        return new DefaultBuilder(Slf4jReporter.forRegistry((MetricRegistry) registry.getMetricRegistry()));
    }

    @Override
    public void start(final long period, final TimeUnit unit) {
        this.slf4jReporter.start(period, unit);
    }

    @Override
    public void report() {
        this.slf4jReporter.report();
    }

    @Override
    public void stop() {
        this.slf4jReporter.stop();
    }

    @Override
    public void close() {
        this.slf4jReporter.close();
    }

    public class DefaultBuilder implements Builder {

        private Slf4jReporter.Builder builder;

        DefaultBuilder(final Slf4jReporter.Builder builder) {
            this.builder = builder;
        }

        @Override
        public Builder shutdownExecutorOnStop(final boolean shutdownExecutorOnStop) {
            return new DefaultBuilder(this.builder.shutdownExecutorOnStop(shutdownExecutorOnStop));
        }

        @Override
        public Builder scheduleOn(final ScheduledExecutorService executor) {
            return new DefaultBuilder(this.builder.scheduleOn(executor));
        }

        @Override
        public Builder outputTo(final Logger logger) {
            return new DefaultBuilder(this.builder.outputTo(logger));
        }

        @Override
        public Builder prefixedWith(final String prefix) {
            return new DefaultBuilder(this.builder.prefixedWith(prefix));
        }

        @Override
        public Builder withLoggingLevel(final LoggingLevel loggingLevel) {
            return new DefaultBuilder(this.builder.withLoggingLevel(loggingLevel(loggingLevel)));
        }

        @Override
        public JRaftSlf4jReporter build() {
            return new DefaultSlf4jReporter(this.builder.build());
        }

        private Slf4jReporter.LoggingLevel loggingLevel(final LoggingLevel loggingLevel) {
            return Arrays.stream(Slf4jReporter.LoggingLevel.values()).filter(
                level -> level.name().equals(loggingLevel.name())).findAny().orElse(null);
        }
    }
}
