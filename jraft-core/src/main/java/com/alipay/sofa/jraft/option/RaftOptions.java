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

/**
 * Raft options.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 4:38:40 PM
 */
public class RaftOptions {

    /** Maximum of block size per RPC */
    private int            maxByteCountPerRpc        = 128 * 1024;
    /** File service check hole switch, default disable */
    private boolean        fileCheckHole             = false;
    /** The maximum number of entries in AppendEntriesRequest */
    private int            maxEntriesSize            = 1024;
    /** The maximum byte size of AppendEntriesRequest */
    private int            maxBodySize               = 512 * 1024;
    /** Flush buffer to LogStorage if the buffer size reaches the limit */
    private int            maxAppendBufferSize       = 256 * 1024;
    /** Maximum election delay time allowed by user */
    private int            maxElectionDelayMs        = 1000;
    /** Raft election:heartbeat timeout factor */
    private int            electionHeartbeatFactor   = 10;
    /** Maximum number of tasks that can be applied in a batch */
    private int            applyBatch                = 32;
    /** Call fsync when need */
    private boolean        sync                      = true;
    /** Sync log meta, snapshot meta and raft meta */
    private boolean        syncMeta                  = false;
    /** Whether to enable replicator pipeline. */
    private boolean        replicatorPipeline        = true;
    /** The maximum replicator pipeline in-flight requests/responses, only valid when enable replicator pipeline. */
    private int            maxReplicatorInflightMsgs = 256;
    /** Internal disruptor buffers size for Node/FSMCaller/LogManager etc. */
    private int            disruptorBufferSize       = 16384;

    /**
     * ReadOnlyOption specifies how the read only request is processed.
     * 
     * {@link ReadOnlyOption#ReadOnlySafe} guarantees the linearizability of the read only request by
     * communicating with the quorum. It is the default and suggested option.

     * {@link ReadOnlyOption#ReadOnlyLeaseBased} ensures linearizability of the read only request by
     * relying on the leader lease. It can be affected by clock drift.
     * If the clock drift is unbounded, leader might keep the lease longer than it
     * should (clock can move backward/pause without any bound). ReadIndex is not safe
     * in that case.
     */
    private ReadOnlyOption readOnlyOptions           = ReadOnlyOption.ReadOnlySafe;

    public ReadOnlyOption getReadOnlyOptions() {
        return readOnlyOptions;
    }

    public void setReadOnlyOptions(ReadOnlyOption readOnlyOptions) {
        this.readOnlyOptions = readOnlyOptions;
    }

    public boolean isReplicatorPipeline() {
        return replicatorPipeline;
    }

    public void setReplicatorPipeline(boolean replicatorPipeline) {
        this.replicatorPipeline = replicatorPipeline;
    }

    public int getMaxReplicatorInflightMsgs() {
        return maxReplicatorInflightMsgs;
    }

    public void setMaxReplicatorInflightMsgs(int maxReplicatorPiplelinePendingResponses) {
        this.maxReplicatorInflightMsgs = maxReplicatorPiplelinePendingResponses;
    }

    public int getDisruptorBufferSize() {
        return this.disruptorBufferSize;
    }

    public void setDisruptorBufferSize(int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
    }

    public int getMaxByteCountPerRpc() {
        return this.maxByteCountPerRpc;
    }

    public void setMaxByteCountPerRpc(int maxByteCountPerRpc) {
        this.maxByteCountPerRpc = maxByteCountPerRpc;
    }

    public boolean isFileCheckHole() {
        return this.fileCheckHole;
    }

    public void setFileCheckHole(boolean fileCheckHole) {
        this.fileCheckHole = fileCheckHole;
    }

    public int getMaxEntriesSize() {
        return this.maxEntriesSize;
    }

    public void setMaxEntriesSize(int maxEntriesSize) {
        this.maxEntriesSize = maxEntriesSize;
    }

    public int getMaxBodySize() {
        return this.maxBodySize;
    }

    public void setMaxBodySize(int maxBodySize) {
        this.maxBodySize = maxBodySize;
    }

    public int getMaxAppendBufferSize() {
        return this.maxAppendBufferSize;
    }

    public void setMaxAppendBufferSize(int maxAppendBufferSize) {
        this.maxAppendBufferSize = maxAppendBufferSize;
    }

    public int getMaxElectionDelayMs() {
        return this.maxElectionDelayMs;
    }

    public void setMaxElectionDelayMs(int maxElectionDelayMs) {
        this.maxElectionDelayMs = maxElectionDelayMs;
    }

    public int getElectionHeartbeatFactor() {
        return this.electionHeartbeatFactor;
    }

    public void setElectionHeartbeatFactor(int electionHeartbeatFactor) {
        this.electionHeartbeatFactor = electionHeartbeatFactor;
    }

    public int getApplyBatch() {
        return this.applyBatch;
    }

    public void setApplyBatch(int applyBatch) {
        this.applyBatch = applyBatch;
    }

    public boolean isSync() {
        return this.sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public boolean isSyncMeta() {
        return this.sync || this.syncMeta;
    }

    public void setSyncMeta(boolean syncMeta) {
        this.syncMeta = syncMeta;
    }

    @Override
    public String toString() {
        return "RaftOptions{" + "maxByteCountPerRpc=" + maxByteCountPerRpc + ", fileCheckHole=" + fileCheckHole
               + ", maxEntriesSize=" + maxEntriesSize + ", maxBodySize=" + maxBodySize + ", maxAppendBufferSize="
               + maxAppendBufferSize + ", maxElectionDelayMs=" + maxElectionDelayMs + ", electionHeartbeatFactor="
               + electionHeartbeatFactor + ", applyBatch=" + applyBatch + ", sync=" + sync + ", syncMeta=" + syncMeta
               + ", replicatorPipeline=" + replicatorPipeline + ", maxReplicatorInflightMsgs="
               + maxReplicatorInflightMsgs + ", disruptorBufferSize=" + disruptorBufferSize + ", readOnlyOptions="
               + readOnlyOptions + '}';
    }
}
