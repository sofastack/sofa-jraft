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
import com.alipay.sofa.jraft.util.metric.JRaftConsoleReporter;
import com.alipay.sofa.jraft.util.metric.JRaftMetricRegistry;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

/**
 * JRaftConsoleReporter default implementation based on {@code ConsoleReporter} of Dropwizard.
 *
 * @author nicholas.jxf
 */
@SPI
public class DefaultConsoleReporter implements JRaftConsoleReporter {

    private ConsoleReporter consoleReporter;

    private DefaultConsoleReporter(final ConsoleReporter consoleReporter) {
        this.consoleReporter = consoleReporter;
    }

    @Override
    public Builder forRegistry(final JRaftMetricRegistry registry) {
        return new DefaultBuilder(ConsoleReporter.forRegistry((MetricRegistry) registry.getMetricRegistry()));
    }

    @Override
    public void start(final long period, final TimeUnit unit) {
        this.consoleReporter.start(period, unit);
    }

    @Override
    public void report() {
        this.consoleReporter.report();
    }

    @Override
    public void stop() {
        this.consoleReporter.stop();
    }

    @Override
    public void close() {
        this.consoleReporter.close();
    }

    public class DefaultBuilder implements Builder {

        private ConsoleReporter.Builder builder;

        DefaultBuilder(final ConsoleReporter.Builder builder) {
            this.builder = builder;
        }

        @Override
        public Builder convertRatesTo(final TimeUnit rateUnit) {
            return new DefaultBuilder(this.builder.convertRatesTo(rateUnit));
        }

        @Override
        public Builder convertDurationsTo(final TimeUnit durationUnit) {
            return new DefaultBuilder(this.builder.convertDurationsTo(durationUnit));
        }

        @Override
        public JRaftConsoleReporter build() {
            return new DefaultConsoleReporter(this.builder.build());
        }
    }
}
