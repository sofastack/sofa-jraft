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
package com.alipay.sofa.jraft.rhea;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import com.alipay.sofa.jraft.rhea.cmd.processor.KVCommandProcessor;
import com.alipay.sofa.jraft.rhea.util.concurrent.CallerRunsPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;

/**
 *
 * @author jiachun.fjc
 */
public final class StoreEngineHelper {

    public static ExecutorService createReadIndexExecutor(final int coreThreads) {
        final int maxThreads = coreThreads << 2;
        final RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();
        return newPool(coreThreads, maxThreads, "rheakv-read-index-callback", handler);
    }

    public static ExecutorService createRaftStateTrigger(final int coreThreads) {
        final BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(32);
        return newPool(coreThreads, coreThreads, "rheakv-raft-state-trigger", workQueue);
    }

    public static ExecutorService createSnapshotExecutor(final int coreThreads, final int maxThreads) {
        return newPool(coreThreads, maxThreads, "rheakv-snapshot-executor");
    }

    public static ExecutorService createCliRpcExecutor(final int coreThreads) {
        final int maxThreads = coreThreads << 2;
        return newPool(coreThreads, maxThreads, "rheakv-cli-rpc-executor");
    }

    public static ExecutorService createRaftRpcExecutor(final int coreThreads) {
        final int maxThreads = coreThreads << 1;
        return newPool(coreThreads, maxThreads, "rheakv-raft-rpc-executor");
    }

    public static ExecutorService createKvRpcExecutor(final int coreThreads) {
        final int maxThreads = coreThreads << 2;
        return newPool(coreThreads, maxThreads, "rheakv-kv-store-rpc-executor");
    }

    public static ScheduledExecutorService createMetricsScheduler() {
        return Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("rheakv-metrics-reporter", true));
    }

    public static void addKvStoreRequestProcessor(final RpcServer rpcServer, final StoreEngine engine) {
        rpcServer.registerProcessor(new KVCommandProcessor(engine));
    }

    private static ExecutorService newPool(final int coreThreads, final int maxThreads, final String name) {
        final RejectedExecutionHandler defaultHandler = new CallerRunsPolicyWithReport(name, name);
        return newPool(coreThreads, maxThreads, name, defaultHandler);
    }

    @SuppressWarnings("SameParameterValue")
    private static ExecutorService newPool(final int coreThreads, final int maxThreads, final String name,
                                           final BlockingQueue<Runnable> workQueue) {
        final RejectedExecutionHandler defaultHandler = new CallerRunsPolicyWithReport(name, name);
        return newPool(coreThreads, maxThreads, workQueue, name, defaultHandler);
    }

    private static ExecutorService newPool(final int coreThreads, final int maxThreads, final String name,
                                           final RejectedExecutionHandler handler) {
        final BlockingQueue<Runnable> defaultWorkQueue = new SynchronousQueue<>();
        return newPool(coreThreads, maxThreads, defaultWorkQueue, name, handler);
    }

    private static ExecutorService newPool(final int coreThreads, final int maxThreads,
                                           final BlockingQueue<Runnable> workQueue, final String name,
                                           final RejectedExecutionHandler handler) {
        return ThreadPoolUtil.newBuilder() //
            .poolName(name) //
            .enableMetric(true) //
            .coreThreads(coreThreads) //
            .maximumThreads(maxThreads) //
            .keepAliveSeconds(60L) //
            .workQueue(workQueue) //
            .threadFactory(new NamedThreadFactory(name, true)) //
            .rejectedHandler(handler) //
            .build();
    }

    private StoreEngineHelper() {
    }
}
