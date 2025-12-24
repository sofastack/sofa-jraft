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

/**
 * Event bus implementation type for internal task queues.
 *
 * @author dennis
 * @see <a href="https://github.com/sofastack/sofa-jraft/issues/1231">Issue #1231</a>
 */
public enum EventBusMode {

    /**
     * Disruptor-based implementation with object reuse.
     * <p>
     * Recommended for:
     * <ul>
     *   <li>ZGC non-generational mode</li>
     *   <li>Shenandoah GC</li>
     *   <li>Low-latency requirements where GC pause is not a concern</li>
     * </ul>
     */
    DISRUPTOR,

    /**
     * MPSC (Multi-Producer Single-Consumer) queue based implementation without object reuse.
     * <p>
     * Recommended for:
     * <ul>
     *   <li>Generational GC (G1, ZGC generational mode)</li>
     *   <li>High QPS scenarios where cross-generation reference overhead is significant</li>
     * </ul>
     * <p>
     * This mode avoids the cross-generation reference problem by not reusing event objects.
     * Each event is a new allocation that gets garbage collected after processing.
     */
    MPSC
}
