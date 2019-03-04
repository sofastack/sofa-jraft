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
package com.alipay.sofa.jraft.rhea.options;

/**
 * @author jiachun.fjc
 */
public class HeartbeatOptions {

    private long storeHeartbeatIntervalSeconds  = 60;
    private long regionHeartbeatIntervalSeconds = 60;
    private int  heartbeatRpcTimeoutMillis      = 5000;

    public long getStoreHeartbeatIntervalSeconds() {
        return storeHeartbeatIntervalSeconds;
    }

    public void setStoreHeartbeatIntervalSeconds(long storeHeartbeatIntervalSeconds) {
        this.storeHeartbeatIntervalSeconds = storeHeartbeatIntervalSeconds;
    }

    public long getRegionHeartbeatIntervalSeconds() {
        return regionHeartbeatIntervalSeconds;
    }

    public void setRegionHeartbeatIntervalSeconds(long regionHeartbeatIntervalSeconds) {
        this.regionHeartbeatIntervalSeconds = regionHeartbeatIntervalSeconds;
    }

    public int getHeartbeatRpcTimeoutMillis() {
        return heartbeatRpcTimeoutMillis;
    }

    public void setHeartbeatRpcTimeoutMillis(int heartbeatRpcTimeoutMillis) {
        this.heartbeatRpcTimeoutMillis = heartbeatRpcTimeoutMillis;
    }

    @Override
    public String toString() {
        return "HeartbeatOptions{" + ", storeHeartbeatIntervalSeconds=" + storeHeartbeatIntervalSeconds
               + ", regionHeartbeatIntervalSeconds=" + regionHeartbeatIntervalSeconds + ", heartbeatRpcTimeoutMillis="
               + heartbeatRpcTimeoutMillis + '}';
    }
}
