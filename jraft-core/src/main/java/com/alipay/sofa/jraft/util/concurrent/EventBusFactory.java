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

import com.alipay.sofa.jraft.util.SPI;

/**
 * Factory interface for creating {@link EventBus} instances.
 * <p>
 * Users can provide custom implementations via:
 * <ul>
 *   <li>SPI mechanism: implement this interface with {@code @SPI(priority = N)} where N > 0</li>
 *   <li>Direct injection: set custom factory via {@code RaftOptions.setEventBusFactory()}</li>
 * </ul>
 *
 * @author dennis
 * @see DefaultEventBusFactory
 * @see <a href="https://github.com/sofastack/sofa-jraft/issues/1231">Issue #1231</a>
 */
@SPI
public interface EventBusFactory {

    /**
     * Create an EventBus instance.
     *
     * @param opts    event bus options
     * @param handler event handler
     * @param <T>     event type
     * @return EventBus instance
     */
    <T> EventBus<T> create(EventBusOptions opts, EventBusHandler<T> handler);
}
