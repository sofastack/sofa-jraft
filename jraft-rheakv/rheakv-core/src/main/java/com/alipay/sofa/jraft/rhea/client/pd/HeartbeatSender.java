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
package com.alipay.sofa.jraft.rhea.client.pd;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.errors.ErrorsHelper;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.metadata.StoreStats;
import com.alipay.sofa.jraft.rhea.metadata.TimeInterval;
import com.alipay.sofa.jraft.rhea.options.HeartbeatOptions;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import com.alipay.sofa.jraft.rhea.storage.BaseKVStoreClosure;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.DiscardOldPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.timer.HashedWheelTimer;
import com.alipay.sofa.jraft.util.timer.Timeout;
import com.alipay.sofa.jraft.util.timer.TimerTask;

/**
 *
 * @author jiachun.fjc
 */
public class HeartbeatSender implements Lifecycle<HeartbeatOptions> {

    private static final Logger         LOG = LoggerFactory.getLogger(HeartbeatSender.class);

    private final StoreEngine           storeEngine;
    private final PlacementDriverClient pdClient;
    private final RpcClient             rpcClient;

    private StatsCollector              statsCollector;
    private InstructionProcessor        instructionProcessor;
    private int                         heartbeatRpcTimeoutMillis;
    private ThreadPoolExecutor          heartbeatRpcCallbackExecutor;
    private HashedWheelTimer            heartbeatTimer;

    private boolean                     started;

    public HeartbeatSender(StoreEngine storeEngine) {
        this.storeEngine = storeEngine;
        this.pdClient = storeEngine.getPlacementDriverClient();
        this.rpcClient = ((AbstractPlacementDriverClient) this.pdClient).getRpcClient();
    }

    @Override
    public synchronized boolean init(final HeartbeatOptions opts) {
        if (this.started) {
            LOG.info("[HeartbeatSender] already started.");
            return true;
        }
        this.statsCollector = new StatsCollector(this.storeEngine);
        this.instructionProcessor = new InstructionProcessor(this.storeEngine);
        this.heartbeatTimer = new HashedWheelTimer(new NamedThreadFactory("heartbeat-timer", true), 50,
            TimeUnit.MILLISECONDS, 4096);
        this.heartbeatRpcTimeoutMillis = opts.getHeartbeatRpcTimeoutMillis();
        if (this.heartbeatRpcTimeoutMillis <= 0) {
            throw new IllegalArgumentException("Heartbeat rpc timeout millis must > 0, "
                                               + this.heartbeatRpcTimeoutMillis);
        }
        final String name = "rheakv-heartbeat-callback";
        this.heartbeatRpcCallbackExecutor = ThreadPoolUtil.newBuilder() //
            .poolName(name) //
            .enableMetric(true) //
            .coreThreads(4) //
            .maximumThreads(4) //
            .keepAliveSeconds(120L) //
            .workQueue(new ArrayBlockingQueue<>(1024)) //
            .threadFactory(new NamedThreadFactory(name, true)) //
            .rejectedHandler(new DiscardOldPolicyWithReport(name)) //
            .build();
        final long storeHeartbeatIntervalSeconds = opts.getStoreHeartbeatIntervalSeconds();
        final long regionHeartbeatIntervalSeconds = opts.getRegionHeartbeatIntervalSeconds();
        if (storeHeartbeatIntervalSeconds <= 0) {
            throw new IllegalArgumentException("Store heartbeat interval seconds must > 0, "
                                               + storeHeartbeatIntervalSeconds);
        }
        if (regionHeartbeatIntervalSeconds <= 0) {
            throw new IllegalArgumentException("Region heartbeat interval seconds must > 0, "
                                               + regionHeartbeatIntervalSeconds);
        }
        final long now = System.currentTimeMillis();
        final StoreHeartbeatTask storeHeartbeatTask = new StoreHeartbeatTask(storeHeartbeatIntervalSeconds, now, false);
        final RegionHeartbeatTask regionHeartbeatTask = new RegionHeartbeatTask(regionHeartbeatIntervalSeconds, now,
            false);
        this.heartbeatTimer.newTimeout(storeHeartbeatTask, storeHeartbeatTask.getNextDelay(), TimeUnit.SECONDS);
        this.heartbeatTimer.newTimeout(regionHeartbeatTask, regionHeartbeatTask.getNextDelay(), TimeUnit.SECONDS);
        LOG.info("[HeartbeatSender] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.heartbeatRpcCallbackExecutor);
        if (this.heartbeatTimer != null) {
            this.heartbeatTimer.stop();
        }
    }

    private void sendStoreHeartbeat(final long nextDelay, final boolean forceRefreshLeader, final long lastTime) {
        final long now = System.currentTimeMillis();
        final StoreHeartbeatRequest request = new StoreHeartbeatRequest();
        request.setClusterId(this.storeEngine.getClusterId());
        final TimeInterval timeInterval = new TimeInterval(lastTime, now);
        final StoreStats stats = this.statsCollector.collectStoreStats(timeInterval);
        request.setStats(stats);
        final HeartbeatClosure<Object> closure = new HeartbeatClosure<Object>() {

            @Override
            public void run(final Status status) {
                final boolean forceRefresh = !status.isOk() && ErrorsHelper.isInvalidPeer(getError());
                final StoreHeartbeatTask nexTask = new StoreHeartbeatTask(nextDelay, now, forceRefresh);
                heartbeatTimer.newTimeout(nexTask, nexTask.getNextDelay(), TimeUnit.SECONDS);
            }
        };
        final Endpoint endpoint = this.pdClient.getPdLeader(forceRefreshLeader, this.heartbeatRpcTimeoutMillis);
        callAsyncWithRpc(endpoint, request, closure);
    }

    private void sendRegionHeartbeat(final long nextDelay, final long lastTime, final boolean forceRefreshLeader) {
        final long now = System.currentTimeMillis();
        final RegionHeartbeatRequest request = new RegionHeartbeatRequest();
        request.setClusterId(this.storeEngine.getClusterId());
        request.setStoreId(this.storeEngine.getStoreId());
        request.setLeastKeysOnSplit(this.storeEngine.getStoreOpts().getLeastKeysOnSplit());
        final List<Long> regionIdList = this.storeEngine.getLeaderRegionIds();
        if (regionIdList.isEmpty()) {
            // So sad, there is no even a region leader :(
            final RegionHeartbeatTask nextTask = new RegionHeartbeatTask(nextDelay, now, false);
            this.heartbeatTimer.newTimeout(nextTask, nextTask.getNextDelay(), TimeUnit.SECONDS);
            if (LOG.isInfoEnabled()) {
                LOG.info("So sad, there is no even a region leader on [clusterId:{}, storeId: {}, endpoint:{}].",
                    this.storeEngine.getClusterId(), this.storeEngine.getStoreId(), this.storeEngine.getSelfEndpoint());
            }
            return;
        }
        final List<Pair<Region, RegionStats>> regionStatsList = Lists.newArrayListWithCapacity(regionIdList.size());
        final TimeInterval timeInterval = new TimeInterval(lastTime, now);
        for (final Long regionId : regionIdList) {
            final Region region = this.pdClient.getRegionById(regionId);
            final RegionStats stats = this.statsCollector.collectRegionStats(region, timeInterval);
            if (stats == null) {
                continue;
            }
            regionStatsList.add(Pair.of(region, stats));
        }
        request.setRegionStatsList(regionStatsList);
        final HeartbeatClosure<List<Instruction>> closure = new HeartbeatClosure<List<Instruction>>() {

            @Override
            public void run(final Status status) {
                final boolean isOk = status.isOk();
                if (isOk) {
                    final List<Instruction> instructions = getResult();
                    if (instructions != null && !instructions.isEmpty()) {
                        instructionProcessor.process(instructions);
                    }
                }
                final boolean forceRefresh = !isOk && ErrorsHelper.isInvalidPeer(getError());
                final RegionHeartbeatTask nextTask = new RegionHeartbeatTask(nextDelay, now, forceRefresh);
                heartbeatTimer.newTimeout(nextTask, nextTask.getNextDelay(), TimeUnit.SECONDS);
            }
        };
        final Endpoint endpoint = this.pdClient.getPdLeader(forceRefreshLeader, this.heartbeatRpcTimeoutMillis);
        callAsyncWithRpc(endpoint, request, closure);
    }

    private <V> void callAsyncWithRpc(final Endpoint endpoint, final BaseRequest request,
                                      final HeartbeatClosure<V> closure) {
        final com.alipay.sofa.jraft.rpc.InvokeContext invokeCtx = new com.alipay.sofa.jraft.rpc.InvokeContext();
        invokeCtx.put(BoltRpcClient.BOLT_CTX, ExtSerializerSupports.getInvokeContext());
        final InvokeCallback invokeCallback = new InvokeCallback() {

            @SuppressWarnings("unchecked")
            @Override
            public void complete(final Object result, final Throwable err) {
                if (err == null) {
                    final BaseResponse<?> response = (BaseResponse<?>) result;
                    if (response.isSuccess()) {
                        closure.setResult((V) response.getValue());
                        closure.run(Status.OK());
                    } else {
                        closure.setError(response.getError());
                        closure.run(new Status(-1, "RPC failed with address: %s, response: %s", endpoint, response));
                    }
                } else {
                    closure.run(new Status(-1, err.getMessage()));
                }
            }

            @Override
            public Executor executor() {
                return heartbeatRpcCallbackExecutor;
            }
        };

        try {
            this.rpcClient.invokeAsync(endpoint, request, invokeCtx, invokeCallback, this.heartbeatRpcTimeoutMillis);
        } catch (final Throwable t) {
            closure.run(new Status(-1, t.getMessage()));
        }
    }

    private static abstract class HeartbeatClosure<V> extends BaseKVStoreClosure {

        private volatile V result;

        public V getResult() {
            return result;
        }

        public void setResult(V result) {
            this.result = result;
        }
    }

    private final class StoreHeartbeatTask implements TimerTask {

        private final long    nextDelay;
        private final long    lastTime;
        private final boolean forceRefreshLeader;

        private StoreHeartbeatTask(long nextDelay, long lastTime, boolean forceRefreshLeader) {
            this.nextDelay = nextDelay;
            this.lastTime = lastTime;
            this.forceRefreshLeader = forceRefreshLeader;
        }

        @Override
        public void run(final Timeout timeout) throws Exception {
            try {
                sendStoreHeartbeat(this.nextDelay, this.forceRefreshLeader, this.lastTime);
            } catch (final Throwable t) {
                LOG.error("Caught a error on sending [StoreHeartbeat]: {}.", StackTraceUtil.stackTrace(t));
            }
        }

        public long getNextDelay() {
            return nextDelay;
        }
    }

    private final class RegionHeartbeatTask implements TimerTask {

        private final long    nextDelay;
        private final long    lastTime;
        private final boolean forceRefreshLeader;

        private RegionHeartbeatTask(long nextDelay, long lastTime, boolean forceRefreshLeader) {
            this.nextDelay = nextDelay;
            this.lastTime = lastTime;
            this.forceRefreshLeader = forceRefreshLeader;
        }

        @Override
        public void run(final Timeout timeout) throws Exception {
            try {
                sendRegionHeartbeat(this.nextDelay, this.lastTime, this.forceRefreshLeader);
            } catch (final Throwable t) {
                LOG.error("Caught a error on sending [RegionHeartbeat]: {}.", StackTraceUtil.stackTrace(t));
            }
        }

        public long getNextDelay() {
            return nextDelay;
        }
    }
}
