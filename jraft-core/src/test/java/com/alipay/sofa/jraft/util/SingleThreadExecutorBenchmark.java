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
package com.alipay.sofa.jraft.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.DefaultEventExecutor;

import com.alipay.sofa.jraft.util.concurrent.DefaultSingleThreadExecutor;
import com.alipay.sofa.jraft.util.concurrent.MpscSingleThreadExecutor;
import com.alipay.sofa.jraft.util.concurrent.SingleThreadExecutor;

/**
 *
 * @author jiachun.fjc
 */
public class SingleThreadExecutorBenchmark {

    private static final SingleThreadExecutor DEFAULT;
    private static final SingleThreadExecutor NEETY_EXECUTOR;
    private static final SingleThreadExecutor MPSC_EXECUTOR;

    private static final int                  TIMES     = 1000000;
    private static final int                  THREADS   = 32;
    private static final Executor             PRODUCERS = ThreadPoolUtil.newBuilder() //
                                                            .coreThreads(THREADS) //
                                                            .maximumThreads(THREADS) //
                                                            .poolName("benchmark") //
                                                            .enableMetric(false) //
                                                            .workQueue(new LinkedBlockingQueue<>()) //
                                                            .keepAliveSeconds(60L) //
                                                            .threadFactory(new NamedThreadFactory("benchmark", true)) //
                                                            .build();

    static {
        DEFAULT = new DefaultSingleThreadExecutor("default", Integer.MAX_VALUE);
        NEETY_EXECUTOR = new DefaultSingleThreadExecutor(new DefaultEventExecutor());
        MPSC_EXECUTOR = new MpscSingleThreadExecutor(Integer.MAX_VALUE, new NamedThreadFactory("mpsc"));
    }

    /*
     * default_single_thread_executor 1302 ms
     * netty_default_event_executor   677 ms
     * mpsc_single_thread_executor    376 ms
     *
     * default_single_thread_executor 1212 ms
     * netty_default_event_executor   979 ms
     * mpsc_single_thread_executor    312 ms
     *
     * default_single_thread_executor 1236 ms
     * netty_default_event_executor   707 ms
     * mpsc_single_thread_executor    381 ms
     */

    public static void main(String[] args) throws InterruptedException {
        final Map<String, SingleThreadExecutor> executors = new LinkedHashMap<>();
        executors.put("default_single_thread_executor ", DEFAULT);
        executors.put("netty_default_event_executor   ", NEETY_EXECUTOR);
        executors.put("mpsc_single_thread_executor    ", MPSC_EXECUTOR);
        final int warmUpTimes = 10000;
        for (final Map.Entry<String, SingleThreadExecutor> entry : executors.entrySet()) {
            final String name = entry.getKey();
            final SingleThreadExecutor executor = entry.getValue();
            // warm-up
            final CountDownLatch warmUpLatch = new CountDownLatch(warmUpTimes);
            for (int i = 0; i < warmUpTimes; i++) {
                execute(executor, warmUpLatch);
            }
            warmUpLatch.await();

            final CountDownLatch benchmarkLatch = new CountDownLatch(TIMES);
            final long start = System.nanoTime();
            for (int i = 0; i < TIMES; i++) {
                PRODUCERS.execute(() -> execute(executor, benchmarkLatch));
            }
            benchmarkLatch.await();

            System.out.println(name + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " ms");

            executor.shutdownGracefully();
        }
    }

    private static void execute(final SingleThreadExecutor executor, final CountDownLatch latch) {
        executor.execute(latch::countDown);
    }
}
