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
package com.alipay.sofa.jraft.rhea.util.concurrent.disruptor;

/**
 * The default wait strategy used by the Disruptor is the BlockingWaitStrategy.
 *
 * BlockingWaitStrategy:
 * Internally the BlockingWaitStrategy uses a typical lock and condition variable to handle thread wake-up.
 * The BlockingWaitStrategy is the slowest of the available wait strategies,
 * but is the most conservative with the respect to CPU usage and will give the most consistent behaviour across
 * the widest variety of deployment options. However, again knowledge of the deployed system can allow for additional
 * performance.
 *
 * SleepingWaitStrategy:
 * Like the BlockingWaitStrategy the SleepingWaitStrategy it attempts to be conservative with CPU usage,
 * by using a simple busy wait loop, but uses a call to LockSupport.parkNanos(1) in the middle of the loop.
 * On a typical Linux system this will pause the thread for around 60µs.
 * However it has the benefit that the producing thread does not need to take any action other increment the appropriate
 * counter and does not require the cost of signalling a condition variable. However, the mean latency of moving the
 * event between the producer and consumer threads will be higher. It works best in situations where low latency is not
 * required, but a low impact on the producing thread is desired. A common use case is for asynchronous logging.
 *
 * YieldingWaitStrategy:
 * The YieldingWaitStrategy is one of 2 Wait Strategies that can be use in low latency systems,
 * where there is the option to burn CPU cycles with the goal of improving latency.
 * The YieldingWaitStrategy will busy spin waiting for the sequence to increment to the appropriate value.
 * Inside the body of the loop Thread.yield() will be called allowing other queued threads to run.
 * This is the recommended wait strategy when need very high performance and the number of Event Handler threads is
 * less than the total number of logical cores, e.g. you have hyper-threading enabled.
 *
 * BusySpinWaitStrategy:
 * The BusySpinWaitStrategy is the highest performing Wait Strategy, but puts the highest constraints on the deployment
 * environment. This wait strategy should only be used if the number of Event Handler threads is smaller than the number
 * of physical cores on the box. E.g. hyper-threading should be disabled
 *
 * @author jiachun.fjc
 */
public enum WaitStrategyType {
    BLOCKING_WAIT, //
    LITE_BLOCKING_WAIT, //
    TIMEOUT_BLOCKING_WAIT, //
    LITE_TIMEOUT_BLOCKING_WAIT, //
    PHASED_BACK_OFF_WAIT, //
    SLEEPING_WAIT, //
    YIELDING_WAIT, //
    BUSY_SPIN_WAIT;

    public static WaitStrategyType parse(String name) {
        for (WaitStrategyType strategy : values()) {
            if (strategy.name().equalsIgnoreCase(name)) {
                return strategy;
            }
        }
        return null;
    }
}
