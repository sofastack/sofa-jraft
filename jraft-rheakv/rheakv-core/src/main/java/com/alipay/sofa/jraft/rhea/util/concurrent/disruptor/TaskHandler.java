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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.WorkHandler;

/**
 * Callback interface to be implemented for processing events
 * as they become available in the RingBuffer.
 *
 * @author jiachun.fjc
 */
public class TaskHandler implements EventHandler<MessageEvent<Runnable>>, WorkHandler<MessageEvent<Runnable>>,
                        TimeoutHandler, LifecycleAware {

    private static final Logger LOG = LoggerFactory.getLogger(TaskHandler.class);

    @Override
    public void onEvent(final MessageEvent<Runnable> event, final long sequence, final boolean endOfBatch)
                                                                                                          throws Exception {
        event.getMessage().run();
        event.reset();
    }

    @Override
    public void onEvent(final MessageEvent<Runnable> event) throws Exception {
        event.getMessage().run();
        event.reset();
    }

    @Override
    public void onTimeout(final long sequence) throws Exception {
        if (LOG.isWarnEnabled()) {
            LOG.warn("Task timeout on: {}, sequence: {}.", Thread.currentThread().getName(), sequence);
        }
    }

    @Override
    public void onStart() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Task handler on start: {}.", Thread.currentThread().getName());
        }
    }

    @Override
    public void onShutdown() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Task handler on shutdown: {}.", Thread.currentThread().getName());
        }
    }
}
