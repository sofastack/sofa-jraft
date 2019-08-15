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

import com.alipay.sofa.jraft.core.BallotBox;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.TimerManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.util.Copiable;

/**
 * Replicator options.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 2:59:24 PM
 */
public class ReplicatorOptions implements Copiable<ReplicatorOptions> {

    private int               dynamicHeartBeatTimeoutMs;
    private int               electionTimeoutMs;
    private String            groupId;
    private PeerId            serverId;
    private PeerId            peerId;
    private LogManager        logManager;
    private BallotBox         ballotBox;
    private NodeImpl          node;
    private long              term;
    private SnapshotStorage   snapshotStorage;
    private RaftClientService raftRpcService;
    private TimerManager      timerManager;

    public ReplicatorOptions() {
        super();
    }

    public ReplicatorOptions(int dynamicHeartBeatTimeoutMs, int electionTimeoutMs, String groupId, PeerId serverId,
                             PeerId peerId, LogManager logManager, BallotBox ballotBox, NodeImpl node, long term,
                             SnapshotStorage snapshotStorage, RaftClientService raftRpcService,
                             TimerManager timerManager) {
        super();
        this.dynamicHeartBeatTimeoutMs = dynamicHeartBeatTimeoutMs;
        this.electionTimeoutMs = electionTimeoutMs;
        this.groupId = groupId;
        this.serverId = serverId;
        if (peerId != null) {
            this.peerId = peerId.copy();
        } else {
            this.peerId = null;
        }
        this.logManager = logManager;
        this.ballotBox = ballotBox;
        this.node = node;
        this.term = term;
        this.snapshotStorage = snapshotStorage;
        this.raftRpcService = raftRpcService;
        this.timerManager = timerManager;
    }

    public RaftClientService getRaftRpcService() {
        return this.raftRpcService;
    }

    public void setRaftRpcService(RaftClientService raftRpcService) {
        this.raftRpcService = raftRpcService;
    }

    @Override
    public ReplicatorOptions copy() {
        final ReplicatorOptions replicatorOptions = new ReplicatorOptions();
        replicatorOptions.setDynamicHeartBeatTimeoutMs(this.dynamicHeartBeatTimeoutMs);
        replicatorOptions.setElectionTimeoutMs(this.electionTimeoutMs);
        replicatorOptions.setGroupId(this.groupId);
        replicatorOptions.setServerId(this.serverId);
        replicatorOptions.setPeerId(this.peerId);
        replicatorOptions.setLogManager(this.logManager);
        replicatorOptions.setBallotBox(this.ballotBox);
        replicatorOptions.setNode(this.node);
        replicatorOptions.setTerm(this.term);
        replicatorOptions.setSnapshotStorage(this.snapshotStorage);
        replicatorOptions.setRaftRpcService(this.raftRpcService);
        replicatorOptions.setTimerManager(this.timerManager);
        return replicatorOptions;
    }

    public TimerManager getTimerManager() {
        return this.timerManager;
    }

    public void setTimerManager(TimerManager timerManager) {
        this.timerManager = timerManager;
    }

    public PeerId getPeerId() {
        return this.peerId;
    }

    public void setPeerId(PeerId peerId) {
        if (peerId != null) {
            this.peerId = peerId.copy();
        } else {
            this.peerId = null;
        }
    }

    public int getDynamicHeartBeatTimeoutMs() {
        return this.dynamicHeartBeatTimeoutMs;
    }

    public void setDynamicHeartBeatTimeoutMs(int dynamicHeartBeatTimeoutMs) {
        this.dynamicHeartBeatTimeoutMs = dynamicHeartBeatTimeoutMs;
    }

    public int getElectionTimeoutMs() {
        return this.electionTimeoutMs;
    }

    public void setElectionTimeoutMs(int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public String getGroupId() {
        return this.groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public PeerId getServerId() {
        return this.serverId;
    }

    public void setServerId(PeerId serverId) {
        this.serverId = serverId;
    }

    public LogManager getLogManager() {
        return this.logManager;
    }

    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public BallotBox getBallotBox() {
        return this.ballotBox;
    }

    public void setBallotBox(BallotBox ballotBox) {
        this.ballotBox = ballotBox;
    }

    public NodeImpl getNode() {
        return this.node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }

    public long getTerm() {
        return this.term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public SnapshotStorage getSnapshotStorage() {
        return this.snapshotStorage;
    }

    public void setSnapshotStorage(SnapshotStorage snapshotStorage) {
        this.snapshotStorage = snapshotStorage;
    }

    @Override
    public String toString() {
        return "ReplicatorOptions{" + "dynamicHeartBeatTimeoutMs=" + dynamicHeartBeatTimeoutMs + ", electionTimeoutMs="
               + electionTimeoutMs + ", groupId='" + groupId + '\'' + ", serverId=" + serverId + ", peerId=" + peerId
               + ", logManager=" + logManager + ", ballotBox=" + ballotBox + ", node=" + node + ", term=" + term
               + ", snapshotStorage=" + snapshotStorage + ", raftRpcService=" + raftRpcService + ", timerManager="
               + timerManager + '}';
    }
}
