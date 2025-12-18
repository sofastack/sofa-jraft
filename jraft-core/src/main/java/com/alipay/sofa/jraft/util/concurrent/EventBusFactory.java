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
package com.alipay.sofa.jraft.util.concurrent;

import com.alipay.sofa.jraft.util.Requires;

/**
 * Factory for creating {@link EventBus} instances.
 *
 * @author dennis
 */
public final class EventBusFactory {

    private EventBusFactory() {
    }

    /**
     * Create an EventBus instance based on the configured mode.
     *
     * @param opts    event bus options
     * @param handler event handler
     * @param <T>     event type
     * @return EventBus instance
     */
    public static <T> EventBus<T> create(final EventBusOptions opts, final EventBusHandler<T> handler) {
        Requires.requireNonNull(opts, "opts");
        Requires.requireNonNull(handler, "handler");

        switch (opts.getMode()) {
            case DISRUPTOR:
                return new DisruptorEventBus<>(opts, handler);
            case MPSC:
                return new MpscEventBus<>(opts, handler);
            default:
                throw new IllegalArgumentException("Unknown EventBusMode: " + opts.getMode());
        }
    }
}
