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
 * Handler for processing events from {@link EventBus}.
 *
 * @param <T> the event type
 * @author dennis
 */
@FunctionalInterface
public interface EventBusHandler<T> {

    /**
     * Process an event.
     *
     * @param event      the event to process
     * @param endOfBatch true if this is the last event in current batch.
     *                   Batch boundaries are determined by:
     *                   <ul>
     *                     <li>Queue is empty (no more events available)</li>
     *                     <li>Reached max batch size limit</li>
     *                   </ul>
     * @throws Exception on processing error
     */
    void onEvent(T event, boolean endOfBatch) throws Exception;
}
