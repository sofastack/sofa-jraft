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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.DefaultEventExecutor;

import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;

/**
 *
 * @author jiachun.fjc
 */
public class SingleThreadExecutorBenchmark {

    private static final SingleThreadExecutor DEFAULT;
    private static final SingleThreadExecutor NETTY_EXECUTOR;
    private static final SingleThreadExecutor MPSC_EXECUTOR;
    private static final SingleThreadExecutor MPSC_EXECUTOR_C_LINKED_QUEUE;
    private static final SingleThreadExecutor MPSC_EXECUTOR_B_LINKED_QUEUE;
    private static final SingleThreadExecutor MPSC_EXECUTOR_T_QUEUE;

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
        NETTY_EXECUTOR = new DefaultSingleThreadExecutor(new DefaultEventExecutor());
        MPSC_EXECUTOR = new MpscSingleThreadExecutor(Integer.MAX_VALUE, new NamedThreadFactory("mpsc"));
        MPSC_EXECUTOR_C_LINKED_QUEUE = new MpscSingleThreadExecutor(Integer.MAX_VALUE, new NamedThreadFactory(
            "mpsc_c_linked_queue")) {

            @Override
            protected Queue<Runnable> getTaskQueue(final int maxPendingTasks) {
                return new ConcurrentLinkedQueue<>();
            }
        };
        MPSC_EXECUTOR_B_LINKED_QUEUE = new MpscSingleThreadExecutor(Integer.MAX_VALUE, new NamedThreadFactory(
            "mpsc_b_linked_queue")) {

            @Override
            protected Queue<Runnable> getTaskQueue(final int maxPendingTasks) {
                return new LinkedBlockingQueue<>(maxPendingTasks);
            }
        };
        MPSC_EXECUTOR_T_QUEUE = new MpscSingleThreadExecutor(Integer.MAX_VALUE, new NamedThreadFactory("mpsc_t_queue")) {

            @Override
            protected Queue<Runnable> getTaskQueue(final int maxPendingTasks) {
                return new LinkedTransferQueue<>();
            }
        };
    }

    /*
     * default_single_thread_executor                      1259 ms
     * netty_default_event_executor                        596 ms
     * mpsc_single_thread_executor                         270 ms
     * mpsc_single_thread_executor_concurrent_linked_queue 324 ms
     * mpsc_single_thread_executor_linked_blocking_queue   535 ms
     * mpsc_single_thread_executor_linked_transfer_queue   322 ms
     *
     * default_single_thread_executor                      1277 ms
     * netty_default_event_executor                        608 ms
     * mpsc_single_thread_executor                         273 ms
     * mpsc_single_thread_executor_concurrent_linked_queue 321 ms
     * mpsc_single_thread_executor_linked_blocking_queue   476 ms
     * mpsc_single_thread_executor_linked_transfer_queue   335 ms
     *
     * default_single_thread_executor                      1235 ms
     * netty_default_event_executor                        619 ms
     * mpsc_single_thread_executor                         265 ms
     * mpsc_single_thread_executor_concurrent_linked_queue 320 ms
     * mpsc_single_thread_executor_linked_blocking_queue   509 ms
     * mpsc_single_thread_executor_linked_transfer_queue   328 ms
     */

    public static void main(String[] args) throws InterruptedException {
        final Map<String, SingleThreadExecutor> executors = new LinkedHashMap<>();
        executors.put("default_single_thread_executor                      ", DEFAULT);
        executors.put("netty_default_event_executor                        ", NETTY_EXECUTOR);
        executors.put("mpsc_single_thread_executor                         ", MPSC_EXECUTOR);
        executors.put("mpsc_single_thread_executor_concurrent_linked_queue ", MPSC_EXECUTOR_C_LINKED_QUEUE);
        executors.put("mpsc_single_thread_executor_linked_blocking_queue   ", MPSC_EXECUTOR_B_LINKED_QUEUE);
        executors.put("mpsc_single_thread_executor_linked_transfer_queue   ", MPSC_EXECUTOR_T_QUEUE);
        final int warmUpTimes = 10000;
        for (final Map.Entry<String, SingleThreadExecutor> entry : executors.entrySet()) {
            final String name = entry.getKey();
            final SingleThreadExecutor executor = entry.getValue();
            // warm-up
            final CountDownLatch warmUpLatch = new CountDownLatch(warmUpTimes);
            for (int i = 0; i < warmUpTimes; i++) {
                PRODUCERS.execute(() -> execute(executor, warmUpLatch));
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
