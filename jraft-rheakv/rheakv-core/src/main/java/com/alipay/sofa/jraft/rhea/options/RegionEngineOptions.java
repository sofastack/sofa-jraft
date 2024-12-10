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

import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Copiable;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 *
 * @author jiachun.fjc
 */
public class RegionEngineOptions implements Copiable<RegionEngineOptions> {

    private Long        regionId;
    private String      startKey;
    private byte[]      startKeyBytes;
    private String      endKey;
    private byte[]      endKeyBytes;
    private NodeOptions nodeOptions;
    // Should extends from StoreEngineOptions
    private String      raftGroupId;
    // Should extends from StoreEngineOptions
    private String      raftDataPath;
    // Should extends from StoreEngineOptions
    private Endpoint    serverAddress;
    // Should extends from StoreEngineOptions
    private String      initialServerList;
    // Can extends from StoreEngineOptions
    private long        metricsReportPeriod;

    public Long getRegionId() {
        return regionId;
    }

    public void setRegionId(Long regionId) {
        this.regionId = regionId;
    }

    public String getStartKey() {
        return startKey;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
        this.startKeyBytes = BytesUtil.writeUtf8(startKey);
    }

    public byte[] getStartKeyBytes() {
        return startKeyBytes;
    }

    public void setStartKeyBytes(byte[] startKeyBytes) {
        this.startKeyBytes = startKeyBytes;
    }

    public String getEndKey() {
        return endKey;
    }

    public void setEndKey(String endKey) {
        this.endKey = endKey;
        this.endKeyBytes = BytesUtil.writeUtf8(endKey);
    }

    public byte[] getEndKeyBytes() {
        return endKeyBytes;
    }

    public void setEndKeyBytes(byte[] endKeyBytes) {
        this.endKeyBytes = endKeyBytes;
    }

    public NodeOptions getNodeOptions() {
        return nodeOptions;
    }

    public void setNodeOptions(NodeOptions nodeOptions) {
        this.nodeOptions = nodeOptions;
    }

    public String getRaftGroupId() {
        return raftGroupId;
    }

    public void setRaftGroupId(String raftGroupId) {
        this.raftGroupId = raftGroupId;
    }

    public String getRaftDataPath() {
        return raftDataPath;
    }

    public void setRaftDataPath(String raftDataPath) {
        this.raftDataPath = raftDataPath;
    }

    public Endpoint getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(Endpoint serverAddress) {
        this.serverAddress = serverAddress;
    }

    public String getInitialServerList() {
        return initialServerList;
    }

    public void setInitialServerList(String initialServerList) {
        this.initialServerList = initialServerList;
    }

    public long getMetricsReportPeriod() {
        return metricsReportPeriod;
    }

    public void setMetricsReportPeriod(long metricsReportPeriod) {
        this.metricsReportPeriod = metricsReportPeriod;
    }

    @Override
    public RegionEngineOptions copy() {
        final RegionEngineOptions copy = new RegionEngineOptions();
        copy.setRegionId(this.regionId);
        copy.setStartKey(this.startKey);
        copy.setEndKey(this.endKey);
        copy.setNodeOptions(this.nodeOptions == null ? new NodeOptions() : this.nodeOptions.copy());
        copy.setRaftGroupId(this.raftGroupId);
        copy.setRaftDataPath(this.raftDataPath);
        copy.setServerAddress(this.serverAddress);
        copy.setInitialServerList(this.initialServerList);
        copy.setMetricsReportPeriod(this.metricsReportPeriod);
        return copy;
    }

    @Override
    public String toString() {
        return "RegionEngineOptions{" + "regionId=" + regionId + ", startKey='" + startKey + '\'' + ", startKeyBytes="
               + BytesUtil.toHex(startKeyBytes) + ", endKey='" + endKey + '\'' + ", endKeyBytes="
               + BytesUtil.toHex(endKeyBytes) + ", raftGroupId='" + raftGroupId + '\'' + ", raftDataPath='"
               + raftDataPath + '\'' + ", nodeOptions=" + nodeOptions + ", serverAddress=" + serverAddress
               + ", initialServerList='" + initialServerList + '\'' + ", metricsReportPeriod=" + metricsReportPeriod
               + '}';
    }
}
