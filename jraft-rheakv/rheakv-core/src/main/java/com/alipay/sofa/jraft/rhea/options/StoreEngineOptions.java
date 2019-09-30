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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;

/**
 *
 * @author jiachun.fjc
 */
public class StoreEngineOptions {

    private StorageType               storageType                   = StorageType.RocksDB;
    private RocksDBOptions            rocksDBOptions;
    private MemoryDBOptions           memoryDBOptions;
    private String                    raftDataPath;
    private Endpoint                  serverAddress;
    // Most configurations do not need to be configured separately for each raft-group,
    // so a common configuration is provided, and each raft-group can copy from here.
    private NodeOptions               commonNodeOptions             = new NodeOptions();
    private List<RegionEngineOptions> regionEngineOptionsList;
    private String                    initialServerList;
    private HeartbeatOptions          heartbeatOptions;
    private boolean                   useSharedRpcExecutor;
    // thread poll number of threads
    private int                       readIndexCoreThreads          = Math.max(Utils.cpus() << 2, 16);
    private int                       leaderStateTriggerCoreThreads = 4;
    private int                       snapshotCoreThreads           = 1;
    private int                       snapshotMaxThreads            = 32;
    private int                       cliRpcCoreThreads             = Utils.cpus() << 2;
    private int                       raftRpcCoreThreads            = Math.max(Utils.cpus() << 3, 32);
    private int                       kvRpcCoreThreads              = Math.max(Utils.cpus() << 3, 32);
    // metrics schedule option (seconds), won't start reporter id metricsReportPeriod <= 0
    private long                      metricsReportPeriod           = TimeUnit.MINUTES.toSeconds(5);
    // the minimum number of keys required to split, less than this value will refuse to split
    private long                      leastKeysOnSplit              = 10000;

    public StorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    public RocksDBOptions getRocksDBOptions() {
        return rocksDBOptions;
    }

    public void setRocksDBOptions(RocksDBOptions rocksDBOptions) {
        this.rocksDBOptions = rocksDBOptions;
    }

    public MemoryDBOptions getMemoryDBOptions() {
        return memoryDBOptions;
    }

    public void setMemoryDBOptions(MemoryDBOptions memoryDBOptions) {
        this.memoryDBOptions = memoryDBOptions;
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

    public NodeOptions getCommonNodeOptions() {
        return commonNodeOptions;
    }

    public void setCommonNodeOptions(NodeOptions commonNodeOptions) {
        this.commonNodeOptions = commonNodeOptions;
    }

    public List<RegionEngineOptions> getRegionEngineOptionsList() {
        return regionEngineOptionsList;
    }

    public void setRegionEngineOptionsList(List<RegionEngineOptions> regionEngineOptionsList) {
        this.regionEngineOptionsList = regionEngineOptionsList;
    }

    public String getInitialServerList() {
        return initialServerList;
    }

    public void setInitialServerList(String initialServerList) {
        this.initialServerList = initialServerList;
    }

    public HeartbeatOptions getHeartbeatOptions() {
        return heartbeatOptions;
    }

    public void setHeartbeatOptions(HeartbeatOptions heartbeatOptions) {
        this.heartbeatOptions = heartbeatOptions;
    }

    public boolean isUseSharedRpcExecutor() {
        return useSharedRpcExecutor;
    }

    public void setUseSharedRpcExecutor(boolean useSharedRpcExecutor) {
        this.useSharedRpcExecutor = useSharedRpcExecutor;
    }

    public int getReadIndexCoreThreads() {
        return readIndexCoreThreads;
    }

    public void setReadIndexCoreThreads(int readIndexCoreThreads) {
        this.readIndexCoreThreads = readIndexCoreThreads;
    }

    public int getLeaderStateTriggerCoreThreads() {
        return leaderStateTriggerCoreThreads;
    }

    public void setLeaderStateTriggerCoreThreads(int leaderStateTriggerCoreThreads) {
        this.leaderStateTriggerCoreThreads = leaderStateTriggerCoreThreads;
    }

    public int getSnapshotCoreThreads() {
        return snapshotCoreThreads;
    }

    public void setSnapshotCoreThreads(int snapshotCoreThreads) {
        this.snapshotCoreThreads = snapshotCoreThreads;
    }

    public int getSnapshotMaxThreads() {
        return snapshotMaxThreads;
    }

    public void setSnapshotMaxThreads(int snapshotMaxThreads) {
        this.snapshotMaxThreads = snapshotMaxThreads;
    }

    public int getCliRpcCoreThreads() {
        return cliRpcCoreThreads;
    }

    public void setCliRpcCoreThreads(int cliRpcCoreThreads) {
        this.cliRpcCoreThreads = cliRpcCoreThreads;
    }

    public int getRaftRpcCoreThreads() {
        return raftRpcCoreThreads;
    }

    public void setRaftRpcCoreThreads(int raftRpcCoreThreads) {
        this.raftRpcCoreThreads = raftRpcCoreThreads;
    }

    public int getKvRpcCoreThreads() {
        return kvRpcCoreThreads;
    }

    public void setKvRpcCoreThreads(int kvRpcCoreThreads) {
        this.kvRpcCoreThreads = kvRpcCoreThreads;
    }

    public long getMetricsReportPeriod() {
        return metricsReportPeriod;
    }

    public void setMetricsReportPeriod(long metricsReportPeriod) {
        this.metricsReportPeriod = metricsReportPeriod;
    }

    public long getLeastKeysOnSplit() {
        return leastKeysOnSplit;
    }

    public void setLeastKeysOnSplit(long leastKeysOnSplit) {
        this.leastKeysOnSplit = leastKeysOnSplit;
    }

    @Override
    public String toString() {
        return "StoreEngineOptions{" + "storageType=" + storageType + ", rocksDBOptions=" + rocksDBOptions
               + ", memoryDBOptions=" + memoryDBOptions + ", raftDataPath='" + raftDataPath + '\'' + ", serverAddress="
               + serverAddress + ", commonNodeOptions=" + commonNodeOptions + ", regionEngineOptionsList="
               + regionEngineOptionsList + ", initialServerList='" + initialServerList + '\'' + ", heartbeatOptions="
               + heartbeatOptions + ", useSharedRpcExecutor=" + useSharedRpcExecutor + ", readIndexCoreThreads="
               + readIndexCoreThreads + ", leaderStateTriggerCoreThreads=" + leaderStateTriggerCoreThreads
               + ", snapshotCoreThreads=" + snapshotCoreThreads + ", snapshotMaxThreads=" + snapshotMaxThreads
               + ", cliRpcCoreThreads=" + cliRpcCoreThreads + ", raftRpcCoreThreads=" + raftRpcCoreThreads
               + ", kvRpcCoreThreads=" + kvRpcCoreThreads + ", metricsReportPeriod=" + metricsReportPeriod
               + ", leastKeysOnSplit=" + leastKeysOnSplit + '}';
    }
}
