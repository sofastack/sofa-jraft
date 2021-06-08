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
package com.alipay.sofa.jraft.rhea.options.configured;

import com.alipay.sofa.jraft.rhea.options.BatchingOptions;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.util.Configured;

/**
 *
 * @author jiachun.fjc
 */
public final class RheaKVStoreOptionsConfigured implements Configured<RheaKVStoreOptions> {

    private final RheaKVStoreOptions opts;

    public static RheaKVStoreOptionsConfigured newConfigured() {
        return new RheaKVStoreOptionsConfigured(new RheaKVStoreOptions());
    }

    public RheaKVStoreOptionsConfigured withClusterId(final long clusterId) {
        this.opts.setClusterId(clusterId);
        return this;
    }

    public RheaKVStoreOptionsConfigured withClusterName(final String clusterName) {
        this.opts.setClusterName(clusterName);
        return this;
    }

    public RheaKVStoreOptionsConfigured withPlacementDriverOptions(final PlacementDriverOptions placementDriverOptions) {
        this.opts.setPlacementDriverOptions(placementDriverOptions);
        return this;
    }

    public RheaKVStoreOptionsConfigured withStoreEngineOptions(final StoreEngineOptions storeEngineOptions) {
        this.opts.setStoreEngineOptions(storeEngineOptions);
        return this;
    }

    public RheaKVStoreOptionsConfigured withInitialServerList(final String initialServerList) {
        this.opts.setInitialServerList(initialServerList);
        return this;
    }

    public RheaKVStoreOptionsConfigured withOnlyLeaderRead(final boolean onlyLeaderRead) {
        this.opts.setOnlyLeaderRead(onlyLeaderRead);
        return this;
    }

    public RheaKVStoreOptionsConfigured withRpcOptions(final RpcOptions rpcOptions) {
        this.opts.setRpcOptions(rpcOptions);
        return this;
    }

    public RheaKVStoreOptionsConfigured withFailoverRetries(final int failoverRetries) {
        this.opts.setFailoverRetries(failoverRetries);
        return this;
    }

    public RheaKVStoreOptionsConfigured withFutureTimeoutMillis(final long futureTimeoutMillis) {
        this.opts.setFutureTimeoutMillis(futureTimeoutMillis);
        return this;
    }

    public RheaKVStoreOptionsConfigured withUseParallelKVExecutor(final boolean useParallelKVExecutor) {
        this.opts.setUseParallelKVExecutor(useParallelKVExecutor);
        return this;
    }

    public RheaKVStoreOptionsConfigured withBatchingOptions(final BatchingOptions batchingOptions) {
        this.opts.setBatchingOptions(batchingOptions);
        return this;
    }

    public RheaKVStoreOptionsConfigured withUseParallelCompress(final boolean useParallelCompress) {
        this.opts.setUseParallelCompress(useParallelCompress);
        return this;
    }

    public RheaKVStoreOptionsConfigured withCompressThreads(final int compressThreads) {
        this.opts.setCompressThreads(compressThreads);
        return this;
    }

    public RheaKVStoreOptionsConfigured withDeCompressThreads(final int deCompressThreads) {
        this.opts.setDeCompressThreads(deCompressThreads);
        return this;
    }

    @Override
    public RheaKVStoreOptions config() {
        return this.opts;
    }

    private RheaKVStoreOptionsConfigured(RheaKVStoreOptions opts) {
        this.opts = opts;
    }
}
