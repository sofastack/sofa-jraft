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

import com.alipay.sofa.jraft.rhea.options.HeartbeatOptions;
import com.alipay.sofa.jraft.rhea.util.Configured;

/**
 *
 * @author jiachun.fjc
 */
public final class HeartbeatOptionsConfigured implements Configured<HeartbeatOptions> {

    private final HeartbeatOptions opts;

    public static HeartbeatOptionsConfigured newConfigured() {
        return new HeartbeatOptionsConfigured(new HeartbeatOptions());
    }

    public HeartbeatOptionsConfigured withStoreHeartbeatIntervalSeconds(final long storeHeartbeatIntervalSeconds) {
        this.opts.setStoreHeartbeatIntervalSeconds(storeHeartbeatIntervalSeconds);
        return this;
    }

    public HeartbeatOptionsConfigured withRegionHeartbeatIntervalSeconds(final long regionHeartbeatIntervalSeconds) {
        this.opts.setRegionHeartbeatIntervalSeconds(regionHeartbeatIntervalSeconds);
        return this;
    }

    public HeartbeatOptionsConfigured withHeartbeatRpcTimeoutMillis(final int heartbeatRpcTimeoutMillis) {
        this.opts.setHeartbeatRpcTimeoutMillis(heartbeatRpcTimeoutMillis);
        return this;
    }

    @Override
    public HeartbeatOptions config() {
        return this.opts;
    }

    private HeartbeatOptionsConfigured(HeartbeatOptions opts) {
        this.opts = opts;
    }
}
