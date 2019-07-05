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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.util.Ints;
import com.alipay.sofa.jraft.util.Requires;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

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
 *
 * @author jiachun.fjc
 */
public class TaskDispatcher implements Dispatcher<Runnable> {

    private static final EventFactory<MessageEvent<Runnable>> eventFactory = MessageEvent::new;

    private final Disruptor<MessageEvent<Runnable>> disruptor;

    public TaskDispatcher(int bufSize, int numWorkers, WaitStrategyType waitStrategyType, ThreadFactory threadFactory) {
        Requires.requireTrue(bufSize > 0, "bufSize must be larger than 0");
        if (!Ints.isPowerOfTwo(bufSize)) {
            bufSize = Ints.roundToPowerOfTwo(bufSize);
        }
        WaitStrategy waitStrategy;
        switch (waitStrategyType) {
            case BLOCKING_WAIT:
                waitStrategy = new BlockingWaitStrategy();
                break;
            case LITE_BLOCKING_WAIT:
                waitStrategy = new LiteBlockingWaitStrategy();
                break;
            case TIMEOUT_BLOCKING_WAIT:
                waitStrategy = new TimeoutBlockingWaitStrategy(1000, TimeUnit.MILLISECONDS);
                break;
            case LITE_TIMEOUT_BLOCKING_WAIT:
                waitStrategy = new LiteTimeoutBlockingWaitStrategy(1000, TimeUnit.MILLISECONDS);
                break;
            case PHASED_BACK_OFF_WAIT:
                waitStrategy = PhasedBackoffWaitStrategy.withLiteLock(1000, 1000, TimeUnit.NANOSECONDS);
                break;
            case SLEEPING_WAIT:
                waitStrategy = new SleepingWaitStrategy();
                break;
            case YIELDING_WAIT:
                waitStrategy = new YieldingWaitStrategy();
                break;
            case BUSY_SPIN_WAIT:
                waitStrategy = new BusySpinWaitStrategy();
                break;
            default:
                throw new UnsupportedOperationException(waitStrategyType.toString());
        }
        this.disruptor = new Disruptor<>(eventFactory, bufSize, threadFactory, ProducerType.MULTI, waitStrategy);
        this.disruptor.setDefaultExceptionHandler(new LoggingExceptionHandler());
        if (numWorkers == 1) {
            this.disruptor.handleEventsWith(new TaskHandler());
        } else {
            final TaskHandler[] handlers = new TaskHandler[numWorkers];
            for (int i = 0; i < numWorkers; i++) {
                handlers[i] = new TaskHandler();
            }
            this.disruptor.handleEventsWithWorkerPool(handlers);
        }
        this.disruptor.start();
    }

    @Override
    public boolean dispatch(final Runnable message) {
        final RingBuffer<MessageEvent<Runnable>> ringBuffer = disruptor.getRingBuffer();
        try {
            final long sequence = ringBuffer.tryNext();
            try {
                final MessageEvent<Runnable> event = ringBuffer.get(sequence);
                event.setMessage(message);
            } finally {
                ringBuffer.publish(sequence);
            }
            return true;
        } catch (final InsufficientCapacityException e) {
            // This exception is used by the Disruptor as a global goto. It is a singleton
            // and has no stack trace.  Don't worry about performance.
            return false;
        }
    }

    @Override
    public void execute(final Runnable message) {
        if (!dispatch(message)) {
            message.run(); // If fail to dispatch, caller run.
        }
    }

    @Override
    public void shutdown() {
        this.disruptor.shutdown();
    }
}
