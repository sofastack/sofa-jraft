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
package com.alipay.sofa.jraft.option;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.util.Copiable;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

/**
 * Raft options.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 4:38:40 PM
 */
public class RaftOptions implements Copiable<RaftOptions> {

    /** Maximum of block size per RPC */
    private int            maxByteCountPerRpc                   = 128 * 1024;
    /** File service check hole switch, default disable */
    private boolean        fileCheckHole                        = false;
    /** The maximum number of entries in AppendEntriesRequest */
    private int            maxEntriesSize                       = 1024;
    /** The maximum byte size of AppendEntriesRequest */
    private int            maxBodySize                          = 512 * 1024;
    /** Flush buffer to LogStorage if the buffer size reaches the limit */
    private int            maxAppendBufferSize                  = 256 * 1024;
    /** Maximum election delay time allowed by user */
    private int            maxElectionDelayMs                   = 1000;
    /** Raft election:heartbeat timeout factor */
    private int            electionHeartbeatFactor              = 10;
    /** Maximum number of tasks that can be applied in a batch */
    private int            applyBatch                           = 32;
    /** Call fsync when need */
    private boolean        sync                                 = true;
    /** Sync log meta, snapshot meta and raft meta */
    private boolean        syncMeta                             = false;
    /** Statistics to analyze the performance of db */
    private boolean        openStatistics                       = true;
    /** Whether to enable replicator pipeline. */
    private boolean        replicatorPipeline                   = true;
    /** The maximum replicator pipeline in-flight requests/responses, only valid when enable replicator pipeline. */
    private int            maxReplicatorInflightMsgs            = 256;
    /** Internal disruptor buffers size for Node/FSMCaller/LogManager etc. */
    private int            disruptorBufferSize                  = 16384;
    /**
     * The maximum timeout in seconds to wait when publishing events into disruptor, default is 10 seconds.
     * If the timeout happens, it may halt the node.
     * */
    private int            disruptorPublishEventWaitTimeoutSecs = 10;
    /**
     *  When true, validate log entry checksum when transferring the log entry from disk or network, default is false.
     *  If true, it would hurt the performance of JRAft but gain the data safety.
     *  @since 1.2.6
     */
    private boolean        enableLogEntryChecksum               = false;

    /**
     * ReadOnlyOption specifies how the read only request is processed.
     * This is a global configuration, you can use {@link Node#readIndex(ReadOnlyOption, byte[], ReadIndexClosure)}
     * to specify how individual requests are processed.
     *
     * {@link ReadOnlyOption#ReadOnlySafe} guarantees the linearizability of the read only request by
     * communicating with the quorum. It is the default and suggested option.

     * {@link ReadOnlyOption#ReadOnlyLeaseBased} ensures linearizability of the read only request by
     * relying on the leader lease. It can be affected by clock drift.
     * If the clock drift is unbounded, leader might keep the lease longer than it
     * should (clock can move backward/pause without any bound). ReadIndex is not safe
     * in that case.
     */
    private ReadOnlyOption readOnlyOptions                      = ReadOnlyOption.ReadOnlySafe;

    /**
     * Read index read need compare current node's apply index with leader's commit index.
     * Only current node's apply index catch up leader's commit index, then call back success to read index closure.
     * Therefore, there is a waiting time. The default wait timeout is 2s. It means that the waiting time
     * over 2s, then call back failure to read index closure. If current node occur problem, it's apply index maybe
     * behind leader's commit index. In read index timeout, it can't catch up, the timeout is waste.
     * Here supply a config to fix it. If the gap greater than maxReadIndexLag, fail fast to call back failure
     * read index closure.
     * @since 1.4.0
     */
    private int            maxReadIndexLag                      = -1;

    /**
     * Candidate steps down when election reaching timeout, default is true(enabled).
     * @since 1.3.0
     */
    private boolean        stepDownWhenVoteTimedout             = true;

    /**
     * Check whether start up old storage (RocksdbLogStorage) when use newLogStorage
     * This option needs to be set to true if logs still exists in RocksdbLogStorage
     */
    private boolean        startupOldStorage                    = false;

    public boolean isStepDownWhenVoteTimedout() {
        return this.stepDownWhenVoteTimedout;
    }

    public void setStepDownWhenVoteTimedout(final boolean stepDownWhenVoteTimeout) {
        this.stepDownWhenVoteTimedout = stepDownWhenVoteTimeout;
    }

    public int getDisruptorPublishEventWaitTimeoutSecs() {
        return this.disruptorPublishEventWaitTimeoutSecs;
    }

    public void setDisruptorPublishEventWaitTimeoutSecs(final int disruptorPublishEventWaitTimeoutSecs) {
        this.disruptorPublishEventWaitTimeoutSecs = disruptorPublishEventWaitTimeoutSecs;
    }

    public boolean isEnableLogEntryChecksum() {
        return this.enableLogEntryChecksum;
    }

    public void setEnableLogEntryChecksum(final boolean enableLogEntryChecksumValidation) {
        this.enableLogEntryChecksum = enableLogEntryChecksumValidation;
    }

    public ReadOnlyOption getReadOnlyOptions() {
        return this.readOnlyOptions;
    }

    public void setReadOnlyOptions(final ReadOnlyOption readOnlyOptions) {
        this.readOnlyOptions = readOnlyOptions;
    }

    public int getMaxReadIndexLag() {
        return maxReadIndexLag;
    }

    public void setMaxReadIndexLag(int maxReadIndexLag) {
        this.maxReadIndexLag = maxReadIndexLag;
    }

    public boolean isReplicatorPipeline() {
        return this.replicatorPipeline && RpcFactoryHelper.rpcFactory().isReplicatorPipelineEnabled();
    }

    public void setReplicatorPipeline(final boolean replicatorPipeline) {
        this.replicatorPipeline = replicatorPipeline;
    }

    public int getMaxReplicatorInflightMsgs() {
        return this.maxReplicatorInflightMsgs;
    }

    public void setMaxReplicatorInflightMsgs(final int maxReplicatorPiplelinePendingResponses) {
        this.maxReplicatorInflightMsgs = maxReplicatorPiplelinePendingResponses;
    }

    public int getDisruptorBufferSize() {
        return this.disruptorBufferSize;
    }

    public void setDisruptorBufferSize(final int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
    }

    public int getMaxByteCountPerRpc() {
        return this.maxByteCountPerRpc;
    }

    public void setMaxByteCountPerRpc(final int maxByteCountPerRpc) {
        this.maxByteCountPerRpc = maxByteCountPerRpc;
    }

    public boolean isFileCheckHole() {
        return this.fileCheckHole;
    }

    public void setFileCheckHole(final boolean fileCheckHole) {
        this.fileCheckHole = fileCheckHole;
    }

    public int getMaxEntriesSize() {
        return this.maxEntriesSize;
    }

    public void setMaxEntriesSize(final int maxEntriesSize) {
        this.maxEntriesSize = maxEntriesSize;
    }

    public int getMaxBodySize() {
        return this.maxBodySize;
    }

    public void setMaxBodySize(final int maxBodySize) {
        this.maxBodySize = maxBodySize;
    }

    public int getMaxAppendBufferSize() {
        return this.maxAppendBufferSize;
    }

    public void setMaxAppendBufferSize(final int maxAppendBufferSize) {
        this.maxAppendBufferSize = maxAppendBufferSize;
    }

    public int getMaxElectionDelayMs() {
        return this.maxElectionDelayMs;
    }

    public void setMaxElectionDelayMs(final int maxElectionDelayMs) {
        this.maxElectionDelayMs = maxElectionDelayMs;
    }

    public int getElectionHeartbeatFactor() {
        return this.electionHeartbeatFactor;
    }

    public void setElectionHeartbeatFactor(final int electionHeartbeatFactor) {
        this.electionHeartbeatFactor = electionHeartbeatFactor;
    }

    public int getApplyBatch() {
        return this.applyBatch;
    }

    public void setApplyBatch(final int applyBatch) {
        this.applyBatch = applyBatch;
    }

    public boolean isSync() {
        return this.sync;
    }

    public void setSync(final boolean sync) {
        this.sync = sync;
    }

    public boolean isSyncMeta() {
        return this.sync || this.syncMeta;
    }

    public void setSyncMeta(final boolean syncMeta) {
        this.syncMeta = syncMeta;
    }

    public boolean isOpenStatistics() {
        return this.openStatistics;
    }

    public void setOpenStatistics(final boolean openStatistics) {
        this.openStatistics = openStatistics;
    }

    public boolean isStartupOldStorage() {
        return startupOldStorage;
    }

    public void setStartupOldStorage(final boolean startupOldStorage) {
        this.startupOldStorage = startupOldStorage;
    }

    @Override
    public RaftOptions copy() {
        final RaftOptions raftOptions = new RaftOptions();
        raftOptions.setMaxByteCountPerRpc(this.maxByteCountPerRpc);
        raftOptions.setFileCheckHole(this.fileCheckHole);
        raftOptions.setMaxEntriesSize(this.maxEntriesSize);
        raftOptions.setMaxBodySize(this.maxBodySize);
        raftOptions.setMaxAppendBufferSize(this.maxAppendBufferSize);
        raftOptions.setMaxElectionDelayMs(this.maxElectionDelayMs);
        raftOptions.setElectionHeartbeatFactor(this.electionHeartbeatFactor);
        raftOptions.setApplyBatch(this.applyBatch);
        raftOptions.setSync(this.sync);
        raftOptions.setSyncMeta(this.syncMeta);
        raftOptions.setOpenStatistics(this.openStatistics);
        raftOptions.setReplicatorPipeline(this.replicatorPipeline);
        raftOptions.setMaxReplicatorInflightMsgs(this.maxReplicatorInflightMsgs);
        raftOptions.setDisruptorBufferSize(this.disruptorBufferSize);
        raftOptions.setDisruptorPublishEventWaitTimeoutSecs(this.disruptorPublishEventWaitTimeoutSecs);
        raftOptions.setEnableLogEntryChecksum(this.enableLogEntryChecksum);
        raftOptions.setReadOnlyOptions(this.readOnlyOptions);
        raftOptions.setStartupOldStorage(this.startupOldStorage);
        return raftOptions;
    }

    @Override
    public String toString() {
        return "RaftOptions{" + "maxByteCountPerRpc=" + maxByteCountPerRpc + ", fileCheckHole=" + fileCheckHole
               + ", maxEntriesSize=" + maxEntriesSize + ", maxBodySize=" + maxBodySize + ", maxAppendBufferSize="
               + maxAppendBufferSize + ", maxElectionDelayMs=" + maxElectionDelayMs + ", electionHeartbeatFactor="
               + electionHeartbeatFactor + ", applyBatch=" + applyBatch + ", sync=" + sync + ", syncMeta=" + syncMeta
               + ", openStatistics=" + openStatistics + ", replicatorPipeline=" + replicatorPipeline
               + ", maxReplicatorInflightMsgs=" + maxReplicatorInflightMsgs + ", disruptorBufferSize="
               + disruptorBufferSize + ", disruptorPublishEventWaitTimeoutSecs=" + disruptorPublishEventWaitTimeoutSecs
               + ", enableLogEntryChecksum=" + enableLogEntryChecksum + ", readOnlyOptions=" + readOnlyOptions
               + ", maxReadIndexLag=" + maxReadIndexLag + ", stepDownWhenVoteTimedout=" + stepDownWhenVoteTimedout
               + ", startUpOldStorage=" + startupOldStorage + '}';
    }
}
