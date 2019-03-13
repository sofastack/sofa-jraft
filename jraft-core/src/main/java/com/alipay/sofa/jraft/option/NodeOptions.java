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

import com.alipay.remoting.util.StringUtils;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Node options.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 2:59:12 PM
 */
public class NodeOptions extends RpcOptions {

    // A follower would become a candidate if it doesn't receive any message
    // from the leader in |election_timeout_ms| milliseconds
    // Default: 1000 (1s)
    private int              electionTimeoutMs      = 1000;                                         // follower to candidate timeout

    // Leader lease time's ratio of electionTimeoutMs,
    // To minimize the effects of clock drift, we should make that:
    // clockDrift + leaderLeaseTimeoutMs < electionTimeout
    // Default: 90, Max: 100
    private int              leaderLeaseTimeRatio   = 90;

    // A snapshot saving would be triggered every |snapshot_interval_s| seconds
    // if this was reset as a positive number
    // If |snapshot_interval_s| <= 0, the time based snapshot would be disabled.
    //
    // Default: 3600 (1 hour)
    private int              snapshotIntervalSecs   = 3600;

    // We will regard a adding peer as caught up if the margin between the
    // last_log_index of this peer and the last_log_index of leader is less than
    // |catchup_margin|
    //
    // Default: 1000
    private int              catchupMargin          = 1000;

    // If node is starting from a empty environment (both LogStorage and
    // SnapshotStorage are empty), it would use |initial_conf| as the
    // configuration of the group, otherwise it would load configuration from
    // the existing environment.
    //
    // Default: A empty group
    private Configuration    initialConf            = new Configuration();

    // The specific StateMachine implemented your business logic, which must be
    // a valid instance.
    private StateMachine     fsm;

    // Describe a specific LogStorage in format ${type}://${parameters}
    private String           logUri;

    // Describe a specific RaftMetaStorage in format ${type}://${parameters}
    private String           raftMetaUri;

    // Describe a specific SnapshotStorage in format ${type}://${parameters}
    private String           snapshotUri;

    // If enable, we will filter duplicate files before copy remote snapshot,
    // to avoid useless transmission. Two files in local and remote are duplicate,
    // only if they has the same filename and the same checksum (stored in file meta).
    // Default: false
    private boolean          filterBeforeCopyRemote = false;

    // If non-null, we will pass this throughput_snapshot_throttle to SnapshotExecutor
    // Default: NULL
    //    scoped_refptr<SnapshotThrottle>* snapshot_throttle;

    // If true, RPCs through raft_cli will be denied.
    // Default: false
    private boolean          disableCli             = false;

    /**
     * Timer manager thread pool size
     */
    private int              timerPoolSize          = Utils.cpus() * 3 > 20 ? 20 : Utils.cpus() * 3;

    /**
     * CLI service request RPC executor pool size, use default executor if -1.
     */
    private int              cliRpcThreadPoolSize   = Utils.cpus();
    /**
     * RAFT request RPC executor pool size, use default executor if -1.
     */
    private int              raftRpcThreadPoolSize  = Utils.cpus() * 6;
    /**
     * Whether to enable metrics for node.
     */
    private boolean          enableMetrics          = false;

    /**
     *  If non-null, we will pass this SnapshotThrottle to SnapshotExecutor
     * Default: NULL
     */
    private SnapshotThrottle snapshotThrottle;

    public SnapshotThrottle getSnapshotThrottle() {
        return snapshotThrottle;
    }

    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }

    /**
     * Raft options
     */
    private RaftOptions raftOptions = new RaftOptions();

    public int getCliRpcThreadPoolSize() {
        return this.cliRpcThreadPoolSize;
    }

    public void setCliRpcThreadPoolSize(int cliRpcThreadPoolSize) {
        this.cliRpcThreadPoolSize = cliRpcThreadPoolSize;
    }

    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public int getRaftRpcThreadPoolSize() {
        return this.raftRpcThreadPoolSize;
    }

    public void setRaftRpcThreadPoolSize(int raftRpcThreadPoolSize) {
        this.raftRpcThreadPoolSize = raftRpcThreadPoolSize;
    }

    public int getTimerPoolSize() {
        return this.timerPoolSize;
    }

    public void setTimerPoolSize(int timerPoolSize) {
        this.timerPoolSize = timerPoolSize;
    }

    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    public void setRaftOptions(RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

    public void validate() {
        if (StringUtils.isBlank(this.logUri)) {
            throw new IllegalArgumentException("Blank logUri");
        }
        if (StringUtils.isBlank(this.raftMetaUri)) {
            throw new IllegalArgumentException("Blank raftMetaUri");
        }
        if (this.fsm == null) {
            throw new IllegalArgumentException("Null stateMachine");
        }
    }

    public int getElectionTimeoutMs() {
        return this.electionTimeoutMs;
    }

    public void setElectionTimeoutMs(int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public int getLeaderLeaseTimeRatio() {
        return leaderLeaseTimeRatio;
    }

    public void setLeaderLeaseTimeRatio(int leaderLeaseTimeRatio) {
        if (leaderLeaseTimeRatio <= 0 || leaderLeaseTimeRatio > 100) {
            throw new IllegalArgumentException("leaderLeaseTimeRatio: " + leaderLeaseTimeRatio
                                               + " (expected: 0 < leaderLeaseTimeRatio <= 100)");
        }
        this.leaderLeaseTimeRatio = leaderLeaseTimeRatio;
    }

    public int getLeaderLeaseTimeoutMs() {
        return this.electionTimeoutMs * this.leaderLeaseTimeRatio / 100;
    }

    public int getSnapshotIntervalSecs() {
        return this.snapshotIntervalSecs;
    }

    public void setSnapshotIntervalSecs(int snapshotIntervalSecs) {
        this.snapshotIntervalSecs = snapshotIntervalSecs;
    }

    public int getCatchupMargin() {
        return this.catchupMargin;
    }

    public void setCatchupMargin(int catchupMargin) {
        this.catchupMargin = catchupMargin;
    }

    public Configuration getInitialConf() {
        return this.initialConf;
    }

    public void setInitialConf(Configuration initialConf) {
        this.initialConf = initialConf;
    }

    public StateMachine getFsm() {
        return this.fsm;
    }

    public void setFsm(StateMachine fsm) {
        this.fsm = fsm;
    }

    public String getLogUri() {
        return this.logUri;
    }

    public void setLogUri(String logUri) {
        this.logUri = logUri;
    }

    public String getRaftMetaUri() {
        return this.raftMetaUri;
    }

    public void setRaftMetaUri(String raftMetaUri) {
        this.raftMetaUri = raftMetaUri;
    }

    public String getSnapshotUri() {
        return this.snapshotUri;
    }

    public void setSnapshotUri(String snapshotUri) {
        this.snapshotUri = snapshotUri;
    }

    public boolean isFilterBeforeCopyRemote() {
        return this.filterBeforeCopyRemote;
    }

    public void setFilterBeforeCopyRemote(boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
    }

    public boolean isDisableCli() {
        return this.disableCli;
    }

    public void setDisableCli(boolean disableCli) {
        this.disableCli = disableCli;
    }

    @Override
    public String toString() {
        return "NodeOptions{" + "electionTimeoutMs=" + electionTimeoutMs + ", snapshotIntervalSecs="
               + snapshotIntervalSecs + ", catchupMargin=" + catchupMargin + ", initialConf=" + initialConf + ", fsm="
               + fsm + ", logUri='" + logUri + '\'' + ", raftMetaUri='" + raftMetaUri + '\'' + ", snapshotUri='"
               + snapshotUri + '\'' + ", filterBeforeCopyRemote=" + filterBeforeCopyRemote + ", disableCli="
               + disableCli + ", timerPoolSize=" + timerPoolSize + ", cliRpcThreadPoolSize=" + cliRpcThreadPoolSize
               + ", raftRpcThreadPoolSize=" + raftRpcThreadPoolSize + ", enableMetrics=" + enableMetrics
               + ", snapshotThrottle=" + snapshotThrottle + ", raftOptions=" + raftOptions + "} " + super.toString();
    }
}
