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

import com.alipay.sofa.jraft.rhea.cmd.store.BatchDeleteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.BatchPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.CASAllRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.CompareAndPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.ContainsKeyRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.DeleteRangeRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.DeleteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.GetAndPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.GetRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.GetSequenceRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.KeyLockRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.KeyUnlockRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.MergeRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.MultiGetRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.NodeExecuteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.PutIfAbsentRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.PutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.RangeSplitRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.ResetSequenceRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.ScanRequest;
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
        rpcServer.registerProcessor(new KVCommandProcessor<>(GetRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(MultiGetRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(ContainsKeyRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(GetSequenceRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(ResetSequenceRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(ScanRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(PutRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(GetAndPutRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(CompareAndPutRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(MergeRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(PutIfAbsentRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(KeyLockRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(KeyUnlockRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(BatchPutRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(DeleteRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(DeleteRangeRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(BatchDeleteRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(NodeExecuteRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(RangeSplitRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(CASAllRequest.class, engine));
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
