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
package com.alipay.sofa.jraft.util.metric;

import org.slf4j.Logger;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Components that implement this interface need to be able to metric slf4j reporter.
 *
 * @author nicholas.jxf
 */
public interface JRaftSlf4jReporter extends JRaftScheduledReporter {

    Builder forRegistry(final JRaftMetricRegistry registry);

    interface Builder {

        Builder shutdownExecutorOnStop(final boolean shutdownExecutorOnStop);

        Builder scheduleOn(final ScheduledExecutorService executor);

        Builder outputTo(final Logger logger);

        Builder prefixedWith(final String prefix);

        Builder withLoggingLevel(final LoggingLevel loggingLevel);

        JRaftSlf4jReporter build();
    }

    enum LoggingLevel {
        TRACE, DEBUG, INFO, WARN, ERROR;

        LoggingLevel() {
        }
    }
}
