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
package com.alipay.sofa.jraft.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.ReadOnlyService;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.closure.SynchronizedClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.Ballot;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.entity.UserLog;
import com.alipay.sofa.jraft.error.LogIndexOutOfBoundsException;
import com.alipay.sofa.jraft.error.LogNotFoundException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.BallotBoxOptions;
import com.alipay.sofa.jraft.option.BootstrapOptions;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.option.LogManagerOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyServiceOptions;
import com.alipay.sofa.jraft.option.ReplicatorGroupOptions;
import com.alipay.sofa.jraft.option.SnapshotExecutorOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.alipay.sofa.jraft.rpc.impl.core.BoltRaftClientService;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.SnapshotExecutor;
import com.alipay.sofa.jraft.storage.StorageFactory;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.RepeatedTimer;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * The raft replica node implementation.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 4:26:51 PM
 */
public class NodeImpl implements Node, RaftServerService {

    private static final Logger            LOG                   = LoggerFactory.getLogger(NodeImpl.class);

    public static final AtomicLong         GLOBAL_NUM_NODES      = new AtomicLong(0);

    /** Internal states */
    private final ReadWriteLock            readWriteLock         = new ReentrantReadWriteLock();
    protected final Lock                   writeLock             = readWriteLock.writeLock();
    protected final Lock                   readLock              = readWriteLock.readLock();
    private volatile State                 state;
    private volatile CountDownLatch        shutdownLatch;
    private long                           currTerm;
    private long                           lastLeaderTimestamp;
    private PeerId                         leaderId              = new PeerId();
    private PeerId                         votedId;
    private final Ballot                   voteCtx               = new Ballot();
    private final Ballot                   prevVoteCtx           = new Ballot();
    private ConfigurationEntry             conf;
    private StopTransferArg                stopTransferArg;
    /** Raft group and node options and identifier */
    private final String                   groupId;
    private NodeOptions                    options;
    private RaftOptions                    raftOptions;
    private final PeerId                   serverId;
    /** Other services */
    private final ConfigurationCtx         confCtx;
    private LogStorage                     logStorage;
    private RaftMetaStorage                metaStorage;
    private ClosureQueue                   closureQueue;
    private ConfigurationManager           configManager;
    private LogManager                     logManager;
    private FSMCaller                      fsmCaller;
    private BallotBox                      ballotBox;
    private SnapshotExecutor               snapshotExecutor;
    private ReplicatorGroup                replicatorGroup;
    private final List<Closure>            shutdownContinuations = new ArrayList<>();
    private RaftClientService              rpcService;
    private ReadOnlyService                readOnlyService;
    /** Timers */
    private TimerManager                   timerManager;
    private RepeatedTimer                  electionTimer;
    private RepeatedTimer                  voteTimer;
    private RepeatedTimer                  stepDownTimer;
    private RepeatedTimer                  snapshotTimer;
    private ScheduledFuture<?>             transferTimer;
    private ThreadId                       wakingCandidate;
    /** disruptor to run node service */
    private Disruptor<LogEntryAndClosure>  applyDisruptor;
    private RingBuffer<LogEntryAndClosure> applyQueue;

    /** metrics*/
    private NodeMetrics                    metrics;

    private NodeId                         nodeId;

    /**
     * Node service event.
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:29:55 PM
     */
    private static class LogEntryAndClosure {
        LogEntry       entry;
        Closure        done;
        long           expectedTerm;
        CountDownLatch shutdownLatch;

        public void reset() {
            this.entry = null;
            this.done = null;
            this.expectedTerm = 0;
            this.shutdownLatch = null;
        }
    }

    private static class LogEntryAndClosureFactory implements EventFactory<LogEntryAndClosure> {

        @Override
        public LogEntryAndClosure newInstance() {
            return new LogEntryAndClosure();
        }
    }

    /**
     * Event handler
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:30:07 PM
     */
    private class LogEntryAndClosureHandler implements EventHandler<LogEntryAndClosure> {
        // task list for batch
        private final List<LogEntryAndClosure> tasks = new ArrayList<>(raftOptions.getApplyBatch());

        @Override
        public void onEvent(LogEntryAndClosure event, long sequence, boolean endOfBatch) throws Exception {
            if (event.shutdownLatch != null) {
                if (!tasks.isEmpty()) {
                    executeApplyingTasks(tasks);
                }
                GLOBAL_NUM_NODES.decrementAndGet();
                event.shutdownLatch.countDown();
                return;
            }

            tasks.add(event);
            if (tasks.size() >= raftOptions.getApplyBatch() || endOfBatch) {
                executeApplyingTasks(tasks);
                tasks.clear();
            }
        }

    }

    /**
     * Configuration commit context.
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:29:38 PM
     */
    private static class ConfigurationCtx {
        enum Stage {
            STAGE_NONE, // none stage
            STAGE_CATCHING_UP, // the node is catching-up.
            STAGE_JOINT, // joint stage
            STAGE_STABLE // stable stage.
        }

        NodeImpl     node;
        Stage        stage;
        int          nchanges;
        long         version;
        List<PeerId> newPeers    = new ArrayList<>();
        List<PeerId> oldPeers    = new ArrayList<>();
        List<PeerId> addingPeers = new ArrayList<>();
        Closure      done;

        public ConfigurationCtx(NodeImpl node) {
            super();
            this.node = node;
            this.stage = Stage.STAGE_NONE;
            this.version = 0;
            this.done = null;
        }

        /**
         * Start change configuration.
         */
        void start(Configuration oldConf, Configuration newConf, Closure done) {
            if (isBusy()) {
                if (done != null) {
                    Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Already in busy stage."));
                }
                throw new IllegalStateException("Busy stage");
            }
            if (this.done != null) {
                if (done != null) {
                    Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "Already have done closure."));
                }
                throw new IllegalArgumentException("Already have done closure.");
            }
            this.done = done;
            this.stage = Stage.STAGE_CATCHING_UP;
            this.oldPeers = oldConf.listPeers();
            this.newPeers = newConf.listPeers();
            final Configuration adding = new Configuration();
            final Configuration removing = new Configuration();
            newConf.diff(oldConf, adding, removing);
            this.nchanges = adding.size() + removing.size();
            if (adding.isEmpty()) {
                nextStage();
                return;
            }
            this.addingPeers = adding.listPeers();
            LOG.info("Adding peers: {}", this.addingPeers);
            for (final PeerId newPeer : this.addingPeers) {
                if (!node.replicatorGroup.addReplicator(newPeer)) {
                    LOG.error("Node {} start the replicator failed, peer is {}", node.getNodeId(), newPeer);
                    this.onCaughtUp(version, newPeer, false);
                    return;
                }
                final OnCaughtUp caughtUp = new OnCaughtUp(node, node.currTerm, newPeer, this.version);
                final long dueTime = Utils.nowMs() + node.options.getElectionTimeoutMs();
                if (!node.replicatorGroup.waitCaughtUp(newPeer, node.options.getCatchupMargin(), dueTime, caughtUp)) {
                    LOG.error("Node {} waitCaughtUp, peer is {}", node.getNodeId(), newPeer);
                    onCaughtUp(version, newPeer, false);
                    return;
                }
            }
        }

        void onCaughtUp(long version, PeerId peer, boolean success) {
            if (version != this.version) {
                return;
            }
            Requires.requireTrue(this.stage == Stage.STAGE_CATCHING_UP, "stage is not in STAGE_CATCHING_UP.");
            if (success) {
                this.addingPeers.remove(peer);
                if (this.addingPeers.isEmpty()) {
                    this.nextStage();
                    return;
                }
                return;
            }
            LOG.warn("Node {} fail to catch up peer {} when trying to change peers from {} to {}.", node.getNodeId(),
                peer, oldPeers, newPeers);
            reset(new Status(RaftError.ECATCHUP, "Peer %s failed to catch up", peer));
        }

        void reset() {
            this.reset(null);
        }

        void reset(Status st) {
            if (st != null && st.isOk()) {
                this.node.stopReplicator(newPeers, oldPeers);
            } else {
                this.node.stopReplicator(oldPeers, newPeers);
            }
            this.newPeers.clear();
            this.oldPeers.clear();
            this.addingPeers.clear();
            this.version++;
            this.stage = Stage.STAGE_NONE;
            this.nchanges = 0;
            if (this.done != null) {
                if (st == null) {
                    st = new Status(RaftError.EPERM, "leader stepped down.");
                }
                Utils.runClosureInThread(done, st);
                this.done = null;
            }
        }

        /**
         *  Invoked when this node becomes the leader, write a configuration change log as the first log
         */
        void flush(Configuration conf, Configuration oldConf) {
            Requires.requireTrue(!isBusy(), "flush when busy");
            this.newPeers = conf.listPeers();
            if (oldConf == null || oldConf.isEmpty()) {
                this.stage = Stage.STAGE_STABLE;
                this.oldPeers = newPeers;
            } else {
                this.stage = Stage.STAGE_JOINT;
                this.oldPeers = oldConf.listPeers();
            }
            this.node.unsafeApplyConfiguration(conf, oldConf == null || oldConf.isEmpty() ? null : oldConf, true);
        }

        void nextStage() {
            Requires.requireTrue(isBusy(), "Not in busy stage");
            switch (this.stage) {
                case STAGE_CATCHING_UP:
                    if (this.nchanges > 1) {
                        this.stage = Stage.STAGE_JOINT;
                        node.unsafeApplyConfiguration(new Configuration(newPeers), new Configuration(oldPeers), false);
                        return;
                    }
                    // Skip joint consensus since only one peers has been changed here. Make
                    // it a one-stage change to be compatible with the legacy
                    // implementation.
                case STAGE_JOINT:
                    this.stage = Stage.STAGE_STABLE;
                    this.node.unsafeApplyConfiguration(new Configuration(newPeers), null, false);
                    break;
                case STAGE_STABLE:
                    final boolean shouldStepDown = !this.newPeers.contains(this.node.serverId);
                    reset(new Status());
                    if (shouldStepDown) {
                        this.node.stepDown(this.node.currTerm, true,
                            new Status(RaftError.ELEADERREMOVED, "This node was removed."));
                    }
                    break;
                case STAGE_NONE:
                    // noinspection ConstantConditions
                    Requires.requireTrue(false, "Can't reach here.");
                    break;
            }
        }

        boolean isBusy() {
            return stage != Stage.STAGE_NONE;
        }
    }

    public NodeImpl() {
        this(null, null);
    }

    public NodeImpl(String groupId, PeerId serverId) {
        super();
        if (groupId != null) {
            Utils.verifyGroupId(groupId);
        }
        this.groupId = groupId;
        this.serverId = serverId != null ? serverId.copy() : null;
        this.state = State.STATE_UNINITIALIZED;
        this.currTerm = 0;
        this.lastLeaderTimestamp = System.currentTimeMillis();
        this.confCtx = new ConfigurationCtx(this);
        this.wakingCandidate = null;
        GLOBAL_NUM_NODES.incrementAndGet();
    }

    private boolean initSnapshotStorage() {
        if (StringUtils.isEmpty(this.options.getSnapshotUri())) {
            LOG.warn("Do not set snapshot uri, ignore initSnapshotStorage.");
            return true;
        }
        this.snapshotExecutor = new SnapshotExecutorImpl();
        final SnapshotExecutorOptions opt = new SnapshotExecutorOptions();
        opt.setUri(options.getSnapshotUri());
        opt.setFsmCaller(this.fsmCaller);
        opt.setNode(this);
        opt.setLogManager(this.logManager);
        opt.setAddr(serverId != null ? serverId.getEndpoint() : null);
        opt.setInitTerm(this.currTerm);
        opt.setFilterBeforeCopyRemote(options.isFilterBeforeCopyRemote());
        // get snapshot throttle
        opt.setSnapshotThrottle(this.options.getSnapshotThrottle());

        return this.snapshotExecutor.init(opt);
    }

    private boolean initLogStorage() {
        Requires.requireNonNull(this.fsmCaller, "null fsm caller.");
        this.logStorage = StorageFactory.createLogStorage(options.getLogUri(), this.raftOptions);
        this.logManager = StorageFactory.createLogManager();
        final LogManagerOptions logManagerOptions = new LogManagerOptions();
        logManagerOptions.setLogStorage(logStorage);
        logManagerOptions.setConfigurationManager(configManager);
        logManagerOptions.setFsmCaller(fsmCaller);
        logManagerOptions.setNodeMetrics(this.metrics);
        logManagerOptions.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
        logManagerOptions.setRaftOptions(this.raftOptions);
        return this.logManager.init(logManagerOptions);
    }

    private boolean initMetaStorage() {
        this.metaStorage = StorageFactory.createRaftMetaStorage(options.getRaftMetaUri(), this.raftOptions,
            this.metrics);
        if (!this.metaStorage.init(null)) {
            LOG.error("Node {} init meta storage failed, uri `{}`", this.serverId, options.getRaftMetaUri());
            return false;
        }
        this.currTerm = this.metaStorage.getTerm();
        this.votedId = this.metaStorage.getVotedFor().copy();
        return true;
    }

    private void handleSnapshotTimeout() {
        writeLock.lock();
        try {
            if (!this.state.isActive()) {
                return;
            }
        } finally {
            writeLock.unlock();
        }
        // do_snapshot in another thread to avoid blocking the timer thread.
        Utils.runInThread(() -> doSnapshot(null));
    }

    private void handleElectionTimeout() {
        boolean doUnlock = true;
        writeLock.lock();
        try {
            if (this.state != State.STATE_FOLLOWER) {
                return;
            }
            if (Utils.nowMs() - this.lastLeaderTimestamp < options.getElectionTimeoutMs()) {
                return;
            }
            final PeerId emptyId = new PeerId();
            this.resetLeaderId(emptyId,
                new Status(RaftError.ERAFTTIMEDOUT, "Lost connection from leader %s", leaderId));
            doUnlock = false;
            this.preVote();
        } finally {
            if (doUnlock) {
                writeLock.unlock();
            }
        }
    }

    private boolean initFSMCaller(LogId bootstrapId) {
        if (this.fsmCaller == null) {
            LOG.error("Fail to init fsm caller, null instance.");
            return false;
        }
        this.closureQueue = new ClosureQueueImpl();
        final FSMCallerOptions fsmCallerOptions = new FSMCallerOptions();
        fsmCallerOptions.setAfterShutdown(status -> afterShutdown());
        fsmCallerOptions.setLogManager(this.logManager);
        fsmCallerOptions.setFsm(this.options.getFsm());
        fsmCallerOptions.setClosureQueue(this.closureQueue);
        fsmCallerOptions.setNode(this);
        fsmCallerOptions.setBootstrapId(bootstrapId);
        fsmCallerOptions.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());

        return this.fsmCaller.init(fsmCallerOptions);
    }

    private static class BootstrapStableClosure extends LogManager.StableClosure {

        public BootstrapStableClosure() {
            super(null);
        }

        private final SynchronizedClosure done = new SynchronizedClosure();

        public Status await() throws InterruptedException {
            return this.done.await();
        }

        @Override
        public void run(Status status) {
            this.done.run(status);
        }

    }

    public boolean bootstrap(BootstrapOptions opts) throws InterruptedException {
        if (opts.getLastLogIndex() > 0) {
            if (opts.getGroupConf().isEmpty() || opts.getFsm() == null) {
                LOG.error("Invalid arguments for bootstrap, groupConf={},fsm={}, while lastLogIndex={}",
                    opts.getGroupConf(), opts.getFsm(), opts.getLastLogIndex());
                return false;
            }
        }
        if (opts.getGroupConf().isEmpty()) {
            LOG.error("Bootstrapping an empty node makes no sense.");
            return false;
        }
        // Term is not an option since changing it is very dangerous
        final long bootstrapLogTerm = opts.getLastLogIndex() > 0 ? 1 : 0;
        final LogId bootstrapId = new LogId(opts.getLastLogIndex(), bootstrapLogTerm);
        this.options = new NodeOptions();
        this.raftOptions = options.getRaftOptions();
        this.metrics = new NodeMetrics(opts.isEnableMetrics());
        this.options.setFsm(opts.getFsm());
        this.options.setLogUri(opts.getLogUri());
        this.options.setRaftMetaUri(opts.getRaftMetaUri());
        this.options.setSnapshotUri(opts.getSnapshotUri());

        this.configManager = new ConfigurationManager();
        // Create fsmCaller at first as logManager needs it to report error
        this.fsmCaller = new FSMCallerImpl();

        if (!initLogStorage()) {
            LOG.error("Fail to init log storage.");
            return false;
        }
        if (!initMetaStorage()) {
            LOG.error("Fail to init meta storage.");
            return false;
        }
        if (currTerm == 0) {
            currTerm = 1;
            if (!this.metaStorage.setTermAndVotedFor(1, new PeerId())) {
                LOG.error("Fail to set term.");
                return false;
            }
        }

        if (opts.getFsm() != null && !initFSMCaller(bootstrapId)) {
            LOG.error("Fail to init fsm caller.");
            return false;
        }

        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.getId().setTerm(currTerm);
        entry.setPeers(opts.getGroupConf().listPeers());

        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);

        final BootstrapStableClosure done = new BootstrapStableClosure();
        this.logManager.appendEntries(entries, done);
        if (!done.await().isOk()) {
            LOG.error("Fail to append configuration.");
            return false;
        }

        if (opts.getLastLogIndex() > 0) {
            if (!initSnapshotStorage()) {
                LOG.error("Fail to init snapshot storage.");
                return false;
            }
            final SynchronizedClosure syncDone = new SynchronizedClosure();
            this.snapshotExecutor.doSnapshot(syncDone);
            if (!syncDone.await().isOk()) {
                LOG.error("Fail to save snapshot: {}.", syncDone.getStatus());
                return false;
            }
        }

        if (opts.getLastLogIndex() > 0) {
            if (logManager.getFirstLogIndex() != opts.getLastLogIndex() + 1) {
                throw new IllegalStateException("first and last log index mismatch");
            }
            if (logManager.getLastLogIndex() != opts.getLastLogIndex()) {
                throw new IllegalStateException("last log index mismatch.");
            }
        } else {
            if (logManager.getFirstLogIndex() != opts.getLastLogIndex() + 1) {
                throw new IllegalStateException("first and last log index mismatch");
            }
            if (logManager.getLastLogIndex() != opts.getLastLogIndex() + 1) {
                throw new IllegalStateException("last log index mismatch.");
            }
        }

        return true;

    }

    private int heartbeatTimeout(int electionTimeout) {
        return Math.max(electionTimeout / raftOptions.getElectionHeartbeatFactor(), 10);
    }

    private int randomTimeout(int timeoutMs) {
        return ThreadLocalRandom.current().nextInt(timeoutMs, timeoutMs + this.raftOptions.getMaxElectionDelayMs());
    }

    @Override
    public boolean init(NodeOptions opts) {
        Requires.requireNonNull(opts, "Null node options");
        Requires.requireNonNull(opts.getRaftOptions(), "Null raft options");
        this.options = opts;
        this.raftOptions = opts.getRaftOptions();
        this.metrics = new NodeMetrics(opts.isEnableMetrics());

        if (this.serverId.getIp().equals(Utils.IP_ANY)) {
            LOG.error("Node can't started from IP_ANY");
            return false;
        }

        if (!NodeManager.getInstance().serverExists(this.serverId.getEndpoint())) {
            LOG.error("No RPC server attached to, did you forget to call addService?");
            return false;
        }

        this.timerManager = new TimerManager();
        if (!this.timerManager.init(this.options.getTimerPoolSize())) {
            LOG.error("Fail to init timer manager");
            return false;
        }
        // Init timers
        this.voteTimer = new RepeatedTimer("JRaft-VoteTimer", this.options.getElectionTimeoutMs()) {

            @Override
            protected void onTrigger() {
                handleVoteTimeout();
            }

            @Override
            protected int adjustTimeout(int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };
        this.electionTimer = new RepeatedTimer("JRaft-ElectionTimer", this.options.getElectionTimeoutMs()) {

            @Override
            protected void onTrigger() {
                handleElectionTimeout();
            }

            @Override
            protected int adjustTimeout(int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };
        this.stepDownTimer = new RepeatedTimer("JRaft-StepDownTimer", this.options.getElectionTimeoutMs()) {

            @Override
            protected void onTrigger() {
                handleStepDownTimeout();
            }
        };
        this.snapshotTimer = new RepeatedTimer("JRaft-SnapshotTimer", this.options.getSnapshotIntervalSecs() * 1000) {

            @Override
            protected void onTrigger() {
                handleSnapshotTimeout();
            }
        };

        this.configManager = new ConfigurationManager();

        this.applyDisruptor = new Disruptor<>(new LogEntryAndClosureFactory(), raftOptions.getDisruptorBufferSize(),
                new NamedThreadFactory("Jraft-NodeImpl-Disruptor-", true));
        this.applyDisruptor.handleEventsWith(new LogEntryAndClosureHandler());
        this.applyDisruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(this.getClass().getSimpleName()));
        this.applyDisruptor.start();
        this.applyQueue = this.applyDisruptor.getRingBuffer();

        this.fsmCaller = new FSMCallerImpl();
        if (!initLogStorage()) {
            LOG.error("Node {} initLogStorage failed.", this.getNodeId());
            return false;
        }
        if (!initMetaStorage()) {
            LOG.error("Node {} initMetaStorage failed.", this.getNodeId());
            return false;
        }
        if (!initFSMCaller(new LogId(0, 0))) {
            LOG.error("Node {} initFSMCaller failed.", this.getNodeId());
            return false;
        }
        this.ballotBox = new BallotBox();
        final BallotBoxOptions ballotBoxOptions = new BallotBoxOptions();
        ballotBoxOptions.setWaiter(this.fsmCaller);
        ballotBoxOptions.setClosureQueue(this.closureQueue);
        if (!ballotBox.init(ballotBoxOptions)) {
            LOG.error("Node {} init ballotBox failed.", this.getNodeId());
            return false;
        }

        if (!this.initSnapshotStorage()) {
            LOG.error("Node {} initSnapshotStorage failed.", this.getNodeId());
            return false;
        }

        final Status st = this.logManager.checkConsistency();
        if (!st.isOk()) {
            LOG.error("Node {} is initialized with inconsitency log:{}", this.getNodeId(), st);
            return false;
        }
        this.conf = new ConfigurationEntry();
        this.conf.setId(new LogId());
        // if have log using conf in log, else using conf in options
        if (logManager.getLastLogIndex() > 0) {
            this.conf = logManager.checkAndSetConfiguration(this.conf);
        } else {
            conf.setConf(options.getInitialConf());
        }

        // RPC service and ReplicatorGroup is in cycle dependent, refactor it TODO
        this.replicatorGroup = new ReplicatorGroupImpl();
        this.rpcService = new BoltRaftClientService(this.replicatorGroup);
        final ReplicatorGroupOptions rgOptions = new ReplicatorGroupOptions();
        rgOptions.setHeartbeatTimeoutMs(heartbeatTimeout(options.getElectionTimeoutMs()));
        rgOptions.setElectionTimeoutMs(options.getElectionTimeoutMs());
        rgOptions.setLogManager(this.logManager);
        rgOptions.setBallotBox(this.ballotBox);
        rgOptions.setNode(this);
        rgOptions.setRaftRpcClientService(this.rpcService);
        rgOptions.setSnapshotStorage(this.snapshotExecutor != null ? this.snapshotExecutor.getSnapshotStorage() : null);
        rgOptions.setRaftOptions(this.raftOptions);
        rgOptions.setTimerManager(timerManager);

        //Adds metric registry to RPC service.
        this.options.setMetricRegistry(this.metrics.getMetricRegistry());

        if (!this.rpcService.init(this.options)) {
            LOG.error("Fail to init rpc service.");
            return false;
        }
        replicatorGroup.init(new NodeId(groupId, serverId), rgOptions);

        this.readOnlyService = new ReadOnlyServiceImpl();
        final ReadOnlyServiceOptions readOnlyServiceOptions = new ReadOnlyServiceOptions();
        readOnlyServiceOptions.setFsmCaller(this.fsmCaller);
        readOnlyServiceOptions.setNode(this);
        readOnlyServiceOptions.setRaftOptions(this.raftOptions);

        if (!this.readOnlyService.init(readOnlyServiceOptions)) {
            LOG.error("Fail to init readOnlyService");
            return false;
        }

        // set state to follower
        this.state = State.STATE_FOLLOWER;

        if (LOG.isInfoEnabled()) {
            LOG.info("Node {} init, term: {}, lastLogId: {}, conf: {}, old_conf: {}", this.getNodeId(), currTerm,
                logManager.getLastLogId(false), conf.getConf(), conf.getOldConf());
        }

        if (this.snapshotExecutor != null && this.options.getSnapshotIntervalSecs() > 0) {
            LOG.debug("Node {} term {} start snapshot timer.", this.getNodeId(), this.currTerm);
            this.snapshotTimer.start();
        }

        if (!this.conf.isEmpty()) {
            stepDown(this.currTerm, false, new Status());
        }

        if (!NodeManager.getInstance().add(this)) {
            LOG.error("NodeManager add {} failed", this.getNodeId());
            return false;
        }

        // Now the raft node is started , have to acquire the writeLock to avoid race
        // conditions
        writeLock.lock();
        if (conf.isStable() && conf.getConf().size() == 1 && conf.getConf().contains(serverId)) {
            // The group contains only this server which must be the LEADER, trigger
            // the timer immediately.
            electSelf();
        } else {
            writeLock.unlock();
        }

        return true;
    }

    // should be in writeLock
    private void electSelf() {
        long oldTerm;
        try {
            LOG.info("Node {} term {} start vote and grant vote self", this.getNodeId(), this.currTerm);
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do electSelf as it is not in {}", this.getNodeId(), this.conf.getConf());
                return;
            }
            if (state == State.STATE_FOLLOWER) {
                LOG.debug("Node {} term {} stop election timer", this.getNodeId(), this.currTerm);
                this.electionTimer.stop();
            }
            final PeerId emptyId = new PeerId();
            resetLeaderId(emptyId, new Status(RaftError.ERAFTTIMEDOUT,
                    "A follower's leader_id is reset to NULL as it begins to request_vote."));
            this.state = State.STATE_CANDIDATE;
            this.currTerm++;
            this.votedId = this.serverId.copy();
            LOG.debug("Node {} term {} start vote_timer", this.getNodeId(), this.currTerm);
            this.voteTimer.start();
            this.voteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            oldTerm = this.currTerm;
        } finally {
            writeLock.unlock();
        }

        final LogId lastLogId = logManager.getLastLogId(true);

        writeLock.lock();
        try {
            // vote need defense ABA after unlock&writeLock
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when getLastLogId.", getNodeId(), this.currTerm);
                return;
            }
            for (final PeerId peer : this.conf.listPeers()) {
                if (peer.equals(this.serverId)) {
                    continue;
                }
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, addr: {}", this.getNodeId(), peer.getEndpoint());
                    continue;
                }
                final OnRequestVoteRpcDone done = new OnRequestVoteRpcDone(peer, this.currTerm, this);
                final RequestVoteRequest.Builder reqBuilder = RequestVoteRequest.newBuilder();
                reqBuilder.setPreVote(false); //It's not a pre-vote request.
                reqBuilder.setGroupId(groupId);
                reqBuilder.setServerId(this.serverId.toString());
                reqBuilder.setPeerId(peer.toString());
                reqBuilder.setTerm(currTerm);
                reqBuilder.setLastLogIndex(lastLogId.getIndex());
                reqBuilder.setLastLogTerm(lastLogId.getTerm());
                done.request = reqBuilder.build();
                rpcService.requestVote(peer.getEndpoint(), done.request, done);
            }

            this.metaStorage.setTermAndVotedFor(this.currTerm, serverId);
            voteCtx.grant(serverId);
            if (voteCtx.isGranted()) {
                becomeLeader();
            }
        } finally {
            writeLock.unlock();
        }

    }

    private void resetLeaderId(PeerId newLeaderId, Status status) {
        if (newLeaderId.isEmpty()) {
            if (!this.leaderId.isEmpty() && state.compareTo(State.STATE_TRANSFERRING) > 0) {
                this.fsmCaller.onStopFollowing(new LeaderChangeContext(this.leaderId.copy(), this.currTerm, status));
            }
            this.leaderId = PeerId.emptyPeer();
        } else {
            if (this.leaderId == null || this.leaderId.isEmpty()) {
                this.fsmCaller.onStartFollowing(new LeaderChangeContext(newLeaderId, this.currTerm, status));
            }
            this.leaderId = newLeaderId.copy();
        }
    }

    // in writeLock
    private void checkStepDown(long requestTerm, PeerId serverId) {
        final Status status = new Status();
        if (requestTerm > currTerm) {
            status.setError(RaftError.ENEWLEADER, "Raft node receives message from new leader with higher term.");
            stepDown(requestTerm, false, status);
        } else if (this.state != State.STATE_FOLLOWER) {
            status.setError(RaftError.ENEWLEADER, "Candidate receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        } else if (this.leaderId.isEmpty()) {
            status.setError(RaftError.ENEWLEADER, "Follower receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        }
        // save current leader
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            resetLeaderId(serverId, status);
        }
    }

    private void becomeLeader() {
        Requires.requireTrue(this.state == State.STATE_CANDIDATE, "Illegal state: " + this.state);
        LOG.info("Node {} term {} become leader of group {} {}", this.getNodeId(), this.currTerm, this.conf.getConf(),
            this.conf.getOldConf());
        // cancel candidate vote timer
        stopVoteTimer();
        this.state = State.STATE_LEADER;
        this.leaderId = this.serverId.copy();
        this.replicatorGroup.resetTerm(this.currTerm);
        for (final PeerId peer : this.conf.listPeers()) {
            if (peer.equals(this.serverId)) {
                continue;
            }
            LOG.debug("Node {} term {} add replicator {}", this.getNodeId(), this.currTerm, peer);
            if (!this.replicatorGroup.addReplicator(peer)) {
                LOG.error("Fail to add replicator for {}", peer);
            }
        }
        // init commit manager
        this.ballotBox.resetPendingIndex(this.logManager.getLastLogIndex() + 1);
        // Register _conf_ctx to reject configuration changing before the first log
        // is committed.
        if (this.confCtx.isBusy()) {
            throw new IllegalStateException();
        }
        this.confCtx.flush(this.conf.getConf(), this.conf.getOldConf());
        this.stepDownTimer.start();
    }

    // should be in writeLock
    private void stepDown(long term, boolean wakeupCandidate, Status status) {
        LOG.debug("Node {} term {} stepDown from {} newTerm {} wakeupCandidate={}", this.getNodeId(), this.currTerm,
            term, wakeupCandidate);
        if (!state.isActive()) {
            return;
        }
        if (state == State.STATE_CANDIDATE) {
            stopVoteTimer();
        } else if (state.compareTo(State.STATE_TRANSFERRING) <= 0) {
            stopStepDownTimer();
            this.ballotBox.clearPendingTasks();
            // signal fsm leader stop immediately
            if (state == State.STATE_LEADER) {
                onLeaderStop(status);
            }
        }
        // reset leader_id
        final PeerId emptyId = new PeerId();
        resetLeaderId(emptyId, status);

        // soft state in memory
        this.state = State.STATE_FOLLOWER;
        this.confCtx.reset();
        this.lastLeaderTimestamp = Utils.nowMs();
        if (this.snapshotExecutor != null) {
            snapshotExecutor.interruptDownloadingSnapshots(term);
        }

        // meta state
        if (term > this.currTerm) {
            this.currTerm = term;
            this.votedId = PeerId.emptyPeer();
            this.metaStorage.setTermAndVotedFor(term, this.votedId);
        }

        if (wakeupCandidate) {
            this.wakingCandidate = this.replicatorGroup.stopAllAndFindTheNextCandidate(this.conf);
            if (this.wakingCandidate != null) {
                Replicator.sendTimeoutNowAndStop(wakingCandidate, options.getElectionTimeoutMs());
            }
        } else {
            replicatorGroup.stopAll();
        }
        if (stopTransferArg != null) {
            if (this.transferTimer != null) {
                this.transferTimer.cancel(true);
            }
            // There is at most one StopTransferTimer at the same term, it's safe to
            // mark stopTransferArg to NULL
            this.stopTransferArg = null;
        }
        this.electionTimer.start();
    }

    private void stopStepDownTimer() {
        if (this.stepDownTimer != null) {
            this.stepDownTimer.stop();
        }
    }

    private void stopVoteTimer() {
        if (this.voteTimer != null) {
            voteTimer.stop();
        }
    }

    class LeaderStableClosure extends LogManager.StableClosure {

        public LeaderStableClosure(List<LogEntry> entries) {
            super(entries);
        }

        @Override
        public void run(Status status) {
            if (status.isOk()) {
                ballotBox.commitAt(this.firstLogIndex, this.firstLogIndex + this.nEntries - 1, serverId);
            } else {
                LOG.error("Node {} append [{}, {}] failed", getNodeId(), firstLogIndex,
                    firstLogIndex + this.nEntries - 1);
            }
        }

    }

    private void executeApplyingTasks(List<LogEntryAndClosure> tasks) {
        writeLock.lock();
        try {
            final int size = tasks.size();
            if (state != State.STATE_LEADER) {
                final Status st = new Status();
                if (state != State.STATE_TRANSFERRING) {
                    st.setError(RaftError.EPERM, "is not leader");
                } else {
                    st.setError(RaftError.EBUSY, "is transferring leadership");
                }
                LOG.debug("Node {} can't apply {}", getNodeId(), st);
                for (int i = 0; i < size; i++) {
                    final LogEntryAndClosure task = tasks.get(i);
                    if (task.done != null) {
                        Utils.runClosureInThread(task.done, st);
                    }
                }
                return;
            }
            final List<LogEntry> entries = new ArrayList<>(tasks.size());
            for (int i = 0; i < size; i++) {
                final LogEntryAndClosure task = tasks.get(i);
                if (task.expectedTerm != -1 && task.expectedTerm != this.currTerm) {
                    LOG.debug("Node {} can't apply task whose expectedTerm={} doesn't match currTerm={}", getNodeId(),
                        task.expectedTerm, this.currTerm);
                    if (task.done != null) {
                        final Status st = new Status(RaftError.EPERM, "expected_term=%d doesn't match current_term=%d",
                            task.expectedTerm, currTerm);
                        Utils.runClosureInThread(task.done, st);
                    }
                    continue;
                }
                // set task entry info before adding to list.
                task.entry.getId().setTerm(currTerm);
                task.entry.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
                entries.add(task.entry);
                if (!this.ballotBox.appendPendingTask(this.conf.getConf(), conf.isStable() ? null : conf.getOldConf(),
                    task.done)) {
                    Utils.runClosureInThread(task.done, new Status(RaftError.EINTERNAL, "Fail to append task."));
                    return;
                }
            }
            this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
            // update conf.first
            this.conf = this.logManager.checkAndSetConfiguration(conf);
        } finally {
            writeLock.unlock();
        }

    }

    /**
     * Get the node metrics.
     *
     * @return returns metrics of current node.
     */
    @Override
    public NodeMetrics getNodeMetrics() {
        return metrics;
    }

    @Override
    public void readIndex(byte[] requestContext, ReadIndexClosure done) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(done, new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down.");
        }
        Requires.requireNonNull(done, "Null closure");
        this.readOnlyService.addRequest(requestContext, done);
    }

    /**
     * ReadIndex response closure
     * @author dennis
     */
    private class ReadIndexHeartbeatResponseClosure extends RpcResponseClosureAdapter<AppendEntriesResponse> {
        ReadIndexResponse.Builder             respBuilder;
        RpcResponseClosure<ReadIndexResponse> closure;
        int                                   quorum;
        int                                   failPeersThreshold;
        int                                   ackSuccess;
        int                                   ackFailures;
        boolean                               isDone;

        public ReadIndexHeartbeatResponseClosure(RpcResponseClosure<ReadIndexResponse> closure,
                                                 ReadIndexResponse.Builder rb, int quorum, int peersCount) {
            super();
            this.quorum = quorum;
            this.closure = closure;
            this.respBuilder = rb;
            this.ackFailures = this.ackSuccess = 0;
            this.isDone = false;
            this.failPeersThreshold = (peersCount % 2 == 0 ? (this.quorum - 1) : this.quorum);
        }

        @Override
        public synchronized void run(Status status) {
            if (isDone) {
                return;
            }
            if (status.isOk() && getResponse().getSuccess()) {
                this.ackSuccess++;
            } else {
                this.ackFailures++;
            }
            //Include leader self vote yes.
            if (ackSuccess + 1 >= this.quorum) {
                this.respBuilder.setSuccess(true);
                closure.setResponse(this.respBuilder.build());
                closure.run(Status.OK());
                isDone = true;
            } else if (this.ackFailures >= failPeersThreshold) {
                this.respBuilder.setSuccess(false);
                closure.setResponse(this.respBuilder.build());
                closure.run(Status.OK());
                isDone = true;
            }
        }
    }

    /**
     * Handle read index request.
     */
    @Override
    public void handleReadIndexRequest(ReadIndexRequest request, RpcResponseClosure<ReadIndexResponse> done) {
        final ReadIndexResponse.Builder respBuilder = ReadIndexResponse.newBuilder();
        final long startMs = Utils.monotonicMs();
        this.readLock.lock();
        try {
            switch (this.state) {
                case STATE_LEADER:
                    readLeader(request, respBuilder, done);
                    break;
                case STATE_FOLLOWER:
                    readFollower(request, done);
                    break;
                case STATE_TRANSFERRING:
                    done.run(new Status(RaftError.EBUSY, "Is transferring leadership"));
                    break;
                default:
                    done.run(new Status(RaftError.EPERM, "Invalid state for readIndex: %s", this.state));
                    break;
            }
        } finally {
            this.readLock.unlock();
            this.metrics.recordLatency("handle-read-index", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-read-index-entries", request.getEntriesCount());
        }
    }

    private int getQuorum() {
        if (this.conf.getConf().isEmpty()) {
            return 0;
        }
        return this.conf.getConf().getPeers().size() / 2 + 1;
    }

    private void readFollower(ReadIndexRequest request, RpcResponseClosure<ReadIndexResponse> closure) {
        if (this.leaderId == null || leaderId.isEmpty()) {
            closure.run(new Status(RaftError.EPERM, "No leader at term %d.", this.currTerm));
        } else {
            // send request to leader.
            final ReadIndexRequest newRequest = ReadIndexRequest.newBuilder(). //
                    mergeFrom(request). //
                    setPeerId(this.leaderId.toString()). //
                    build();
            this.rpcService.readIndex(this.leaderId.getEndpoint(), newRequest, -1, closure);
        }
    }

    private void readLeader(ReadIndexRequest request, final ReadIndexResponse.Builder respBuilder,
                            RpcResponseClosure<ReadIndexResponse> closure) {
        final int quorum = getQuorum();
        if (quorum > 1) {
            final long lastCommittedIndex = this.ballotBox.getLastCommittedIndex();
            if (this.logManager.getTerm(lastCommittedIndex) != this.currTerm) {
                // Reject read only request when this leader has not committed any log entry at its term
                closure.run(new Status(RaftError.EAGAIN,
                    "ReadIndex request rejected because leader has not committed any log entry at its term, logIndex=%d, currTerm=%d",
                    lastCommittedIndex, currTerm));
                return;
            }
            respBuilder.setIndex(lastCommittedIndex);

            if (request.getPeerId() != null) {
                // request from follower, check if the follower is in current conf.
                final PeerId peer = new PeerId();
                peer.parse(request.getServerId());
                if (!getConf().contains(peer)) {
                    closure.run(new Status(RaftError.EPERM, "Peer %s is not in current configuration: {}", peer,
                        this.getConf()));
                    return;
                }
            }

            switch (this.raftOptions.getReadOnlyOptions()) {
                case ReadOnlySafe:
                    final ReadIndexHeartbeatResponseClosure heartbeatDone = new ReadIndexHeartbeatResponseClosure(
                        closure, respBuilder, quorum, this.conf.getConf().getPeers().size());
                    final List<PeerId> peers = this.conf.getConf().getPeers();

                    Requires.requireNonNull(peers, "Peer is null");
                    Requires.requireTrue(!peers.isEmpty(), "Peer is empty");
                    // Send heartbeat requests to followers
                    for (final PeerId peer : peers) {
                        if (peer.equals(this.serverId)) {
                            continue;
                        }
                        this.replicatorGroup.sendHeartbeat(peer, heartbeatDone);
                    }
                    break;
                case ReadOnlyLeaseBased:
                    // Responses to followers and local node.
                    respBuilder.setSuccess(true);
                    closure.setResponse(respBuilder.build());
                    closure.run(Status.OK());
                    break;
            }
        } else {
            // Only one peer, fast path.
            respBuilder.setSuccess(true);
            respBuilder.setIndex(this.ballotBox.getLastCommittedIndex());
            closure.setResponse(respBuilder.build());
            closure.run(Status.OK());
        }
    }

    @Override
    public void apply(final Task task) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down.");
        }
        Requires.requireNonNull(task, "Null task");

        final LogEntry entry = new LogEntry();
        entry.setData(task.getData());

        try {
            this.applyQueue.publishEvent((event, sequence) -> {
                event.reset();
                event.done = task.getDone();
                event.entry = entry;
                event.expectedTerm = task.getExpectedTerm();
            });
        } catch (final Exception e) {
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.EPERM, "Node is down."));
        }
    }

    @Override
    public Message handlePreVoteRequest(RequestVoteRequest request) {
        boolean doUnlock = true;
        writeLock.lock();
        try {
            if (!state.isActive()) {
                LOG.warn("Node {} is not in active state, current term {}", this.getNodeId(), this.currTerm);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s is not in active state, state %s.",
                    getNodeId(), this.state.name());
            }
            final PeerId candidateId = new PeerId();
            if (!candidateId.parse(request.getServerId())) {
                LOG.warn("Node {} received PreVote from {} serverId bad format", this.getNodeId(),
                    request.getServerId());
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse candidateId failed: %s.",
                    request.getServerId());
            }
            boolean granted = false;
            // noinspection ConstantConditions
            do {
                if (request.getTerm() < this.currTerm) {
                    LOG.info("Node {} ignore PreVote from {} in term {} currTerm {}", this.getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    checkReplicator(candidateId);
                    break;
                } else if (request.getTerm() == this.currTerm + 1) {
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    // check replicator state
                    checkReplicator(candidateId);
                }
                doUnlock = false;
                this.writeLock.unlock();

                final LogId logId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                final LogId requestLastLogId = new LogId(request.getLastLogIndex(), request.getLastLogTerm());
                granted = (requestLastLogId.compareTo(logId) >= 0);

                LOG.info(
                    "Node {} received PreVote from {} in term {} currTerm {} granted {}, request last logId: {}, current last logId: {}",
                    this.getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, granted,
                    requestLastLogId, logId);
            } while (false);

            final RequestVoteResponse.Builder responseBuilder = RequestVoteResponse.newBuilder();
            responseBuilder.setTerm(this.currTerm);
            responseBuilder.setGranted(granted);
            return responseBuilder.build();
        } finally {
            if (doUnlock) {
                writeLock.unlock();
            }
        }
    }

    private void checkReplicator(PeerId candidateId) {
        if (this.state == State.STATE_LEADER) {
            this.replicatorGroup.checkReplicator(candidateId, false);
        }
    }

    @Override
    public Message handleRequestVoteRequest(RequestVoteRequest request) {
        boolean doUnlock = true;
        writeLock.lock();
        try {
            if (!state.isActive()) {
                LOG.warn("Node {} is not in active state, current term {}", this.getNodeId(), this.currTerm);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s is not in active state, state %s.",
                    getNodeId(), this.state.name());
            }
            final PeerId candidateId = new PeerId();
            if (!candidateId.parse(request.getServerId())) {
                LOG.warn("Node {} received RequestVoteRequest from {} serverId bad format", this.getNodeId(),
                    request.getServerId());
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse candidateId failed: %s.",
                    request.getServerId());
            }

            // noinspection ConstantConditions
            do {
                // check term
                if (request.getTerm() >= this.currTerm) {
                    LOG.info("Node {} received RequestVoteRequest from {} in term {} currTerm {}", this.getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                    // increase current term, change state to follower
                    if (request.getTerm() > this.currTerm) {
                        this.stepDown(request.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                                "Raft node receives higher term RequestVoteRequest."));
                    }
                } else {
                    // ignore older term
                    LOG.info("Node {} ignore RequestVoteRequest from {} in term {} currTerm {}", this.getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                    break;
                }
                doUnlock = false;
                writeLock.unlock();

                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                writeLock.lock();
                // vote need ABA check after unlock&writeLock
                if (request.getTerm() != this.currTerm) {
                    LOG.warn("Node {} raise term {} when get lastLogId", this.getNodeId(), this.currTerm);
                    break;
                }
                final boolean logIsOk = new LogId(request.getLastLogIndex(), request.getLastLogTerm())
                        .compareTo(lastLogId) >= 0;

                        if (logIsOk && (votedId == null || this.votedId.isEmpty())) {
                            this.stepDown(request.getTerm(), false, new Status(RaftError.EVOTEFORCANDIDATE,
                                    "Raft node votes for some candidate, step down to restart election_timer."));
                            this.votedId = candidateId.copy();
                            this.metaStorage.setVotedFor(candidateId);
                        }

            } while (false);

            final RequestVoteResponse.Builder responseBuilder = RequestVoteResponse.newBuilder();
            responseBuilder.setTerm(this.currTerm);
            responseBuilder.setGranted(request.getTerm() == this.currTerm && this.votedId.equals(candidateId));
            return responseBuilder.build();
        } finally {
            if (doUnlock) {
                writeLock.unlock();
            }
        }
    }

    private static class FollowerStableClosure extends LogManager.StableClosure {

        final long                          committedIndex;
        final AppendEntriesResponse.Builder responseBuilder;
        final NodeImpl                      node;
        final RpcRequestClosure             done;
        final long                          term;

        public FollowerStableClosure(AppendEntriesRequest request, AppendEntriesResponse.Builder responseBuilder,
                                     NodeImpl node, RpcRequestClosure done, long term) {
            super(null);
            this.committedIndex = Math.min(
                // committed index is likely less than the lastLogIndex
                request.getCommittedIndex(),
                // The logs after the appended entries can not be trust, so we can't commit them even if their indexes are less than request's committed index.
                request.getPrevLogIndex() + request.getEntriesCount());
            this.responseBuilder = responseBuilder;
            this.node = node;
            this.done = done;
            this.term = term;
        }

        @Override
        public void run(Status status) {

            if (!status.isOk()) {
                this.done.run(status);
                return;
            }

            node.readLock.lock();
            try {
                if (term != this.node.currTerm) {
                    // The change of term indicates that leader has been changed during
                    // appending entries, so we can't respond ok to the old leader
                    // because we are not sure if the appended logs would be truncated
                    // by the new leader:
                    //  - If they won't be truncated and we respond failure to the old
                    //    leader, the new leader would know that they are stored in this
                    //    peer and they will be eventually committed when the new leader
                    //    found that quorum of the cluster have stored.
                    //  - If they will be truncated and we responded success to the old
                    //    leader, the old leader would possibly regard those entries as
                    //    committed (very likely in a 3-nodes cluster) and respond
                    //    success to the clients, which would break the rule that
                    //    committed entries would never be truncated.
                    // So we have to respond failure to the old leader and set the new
                    // term to make it stepped down if it didn't.
                    responseBuilder.setSuccess(false);
                    responseBuilder.setTerm(node.currTerm);
                    this.done.sendResponse(responseBuilder.build());
                    return;
                }
            } finally {
                // It's safe to release lock as we know everything is ok at this point.
                node.readLock.unlock();
            }

            // DON'T touch node any more
            responseBuilder.setSuccess(true);
            responseBuilder.setTerm(this.term);

            //ballot box is thread safe and tolerates disorder.
            this.node.ballotBox.setLastCommittedIndex(this.committedIndex);

            this.done.sendResponse(responseBuilder.build());
        }
    }

    @Override
    public Message handleAppendEntriesRequest(AppendEntriesRequest request, RpcRequestClosure done) {
        boolean doUnlock = true;
        final long startMs = Utils.monotonicMs();
        writeLock.lock();
        final int entriesCount = request.getEntriesCount();
        try {
            final AppendEntriesResponse.Builder responseBuilder = AppendEntriesResponse.newBuilder();
            responseBuilder.setTerm(this.currTerm);

            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, current term {}", this.getNodeId(), this.currTerm);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s is not in active state, state %s.",
                    getNodeId(), this.state.name());
            }

            final PeerId serverId = new PeerId();
            if (!serverId.parse(request.getServerId())) {
                LOG.warn("Node {} received AppendEntriesRequest from {} serverId bad format", this.getNodeId(),
                    request.getServerId());
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse serverId failed: %s.",
                    request.getServerId());
            }

            // check stale term
            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale AppendEntriesRequest from {} in term {} currTerm {}", this.getNodeId(),
                    request.getServerId(), request.getTerm(), this.currTerm);
                responseBuilder.setSuccess(false);
                responseBuilder.setTerm(this.currTerm);
                return responseBuilder.build();
            }

            // check term and state to step down
            this.checkStepDown(request.getTerm(), serverId);
            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer={} declares that it is the leader at term={} which was occupied by leader={}",
                    serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                stepDown(request.getTerm() + 1, false,
                    new Status(RaftError.ELEADERCONFLICT, "More than one leader in the same term."));
                responseBuilder.setSuccess(false);
                responseBuilder.setTerm(request.getTerm() + 1);
                return responseBuilder.build();
            }

            this.lastLeaderTimestamp = Utils.nowMs();

            if (entriesCount > 0 && this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn("Node {} received AppendEntriesRequest while installing snapshot", getNodeId());
                return RpcResponseFactory.newResponse(RaftError.EBUSY, "Node %s:%s is installing snapshot.",
                    this.groupId, this.serverId);
            }

            final long prevLogIndex = request.getPrevLogIndex();
            final long prevLogTerm = request.getPrevLogTerm();
            final long localPrevLogTerm = logManager.getTerm(prevLogIndex);
            if (localPrevLogTerm != prevLogTerm) {
                final long lastLogIndex = logManager.getLastLogIndex();
                LOG.warn(
                    "Node {} reject term_unmatched AppendEntriesRequest from {} in term {} prevLogIndex {} prevLogTerm {} localPrevLogTerm {} lastLogIndex {} entriesSize {}",
                    this.getNodeId(), request.getServerId(), request.getTerm(), prevLogIndex, prevLogTerm,
                    localPrevLogTerm, lastLogIndex, entriesCount);

                responseBuilder.setSuccess(false);
                responseBuilder.setTerm(currTerm);
                responseBuilder.setLastLogIndex(lastLogIndex);
                return responseBuilder.build();
            }

            if (entriesCount == 0) {
                // heartbeat
                responseBuilder.setSuccess(true);
                responseBuilder.setTerm(currTerm);
                responseBuilder.setLastLogIndex(logManager.getLastLogIndex());
                doUnlock = false;
                writeLock.unlock();
                // see the comments at FollowerStableClosure#run()
                this.ballotBox.setLastCommittedIndex(Math.min(request.getCommittedIndex(), prevLogIndex));
                return responseBuilder.build();
            }

            // Parse request
            long index = prevLogIndex;
            final List<LogEntry> entries = new ArrayList<>(entriesCount);
            ByteBuffer allData = null;
            if (request.hasData()) {
                allData = request.getData().asReadOnlyByteBuffer();
            }

            final List<RaftOutter.EntryMeta> entriesList = request.getEntriesList();
            for (int i = 0; i < entriesCount; i++) {
                final RaftOutter.EntryMeta entry = entriesList.get(i);
                index++;
                if (entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_UNKNOWN) {
                    final LogEntry logEntry = new LogEntry();
                    logEntry.setId(new LogId(index, entry.getTerm()));
                    logEntry.setType(entry.getType());
                    final long dataLen = entry.getDataLen();
                    if (dataLen > 0) {
                        final byte[] bs = new byte[(int) dataLen];
                        assert allData != null;
                        allData.get(bs, 0, bs.length);
                        logEntry.setData(ByteBuffer.wrap(bs));
                    }

                    if (entry.getPeersCount() > 0) {
                        if (entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                            throw new IllegalStateException();
                        }

                        final List<PeerId> peers = new ArrayList<>(entry.getPeersCount());
                        for (final String peerStr : entry.getPeersList()) {
                            final PeerId peer = new PeerId();
                            peer.parse(peerStr);
                            peers.add(peer);
                        }
                        logEntry.setPeers(peers);

                        if (entry.getOldPeersCount() > 0) {
                            final List<PeerId> oldPeers = new ArrayList<>(entry.getOldPeersCount());
                            for (final String peerStr : entry.getOldPeersList()) {
                                final PeerId peer = new PeerId();
                                peer.parse(peerStr);
                                oldPeers.add(peer);
                            }
                            logEntry.setOldPeers(oldPeers);
                        }

                    } else {
                        if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                            throw new IllegalStateException(
                                    "Invalid log entry that contains zero peers but is ENTRY_TYPE_CONFIGURATION type.");
                        }
                    }

                    entries.add(logEntry);
                }
            }

            final FollowerStableClosure c = new FollowerStableClosure(request, responseBuilder, this, done,
                this.currTerm);
            logManager.appendEntries(entries, c);
            // update configuration after _log_manager updated its memory status
            this.conf = logManager.checkAndSetConfiguration(this.conf);
            return null;
        } finally {
            if (doUnlock) {
                writeLock.unlock();
            }
            this.metrics.recordLatency("handle-append-entries", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-append-entries-count", entriesCount);
        }
    }

    // called when leader recv greater term in AppendEntriesResponse
    void increaseTermTo(long newTerm, Status status) {
        writeLock.lock();
        try {
            if (newTerm < this.currTerm) {
                return;
            }
            this.stepDown(newTerm, false, status);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Peer catch up callback
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-11 2:10:02 PM
     */
    private static class OnCaughtUp extends CatchUpClosure {
        private final NodeImpl node;
        private final long     term;
        private final PeerId   peer;
        private final long     version;

        public OnCaughtUp(NodeImpl node, long term, PeerId peer, long version) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
            this.version = version;
        }

        @Override
        public void run(Status status) {
            this.node.onCaughtUp(peer, term, version, status);
        }
    }

    private void onCaughtUp(PeerId peer, long term, long version, Status st) {
        this.writeLock.lock();
        try {
            // CHECK _current_term and _state to avoid ABA problem
            if (term != currTerm && state != State.STATE_LEADER) {
                // term has changed and nothing should be done, otherwise there will be
                // an ABA problem.
                return;
            }
            if (st.isOk()) {
                // Caught up successfully
                confCtx.onCaughtUp(version, peer, true);
                return;
            }
            // Retry if this peer is still alive
            if (st.getCode() == RaftError.ETIMEDOUT.getNumber()
                    && Utils.nowMs() - replicatorGroup.getLastRpcSendTimestamp(peer) <= options.getElectionTimeoutMs()) {
                LOG.debug("Node {} waits peer {} to catch up", getNodeId(), peer);
                final OnCaughtUp caughtUp = new OnCaughtUp(this, term, peer, version);
                final long dueTime = Utils.nowMs() + options.getElectionTimeoutMs();
                if (replicatorGroup.waitCaughtUp(peer, options.getCatchupMargin(), dueTime, caughtUp)) {
                    return;
                } else {
                    LOG.warn("Node {} waitCaughtUp failed, peer {}", getNodeId(), peer);
                }

            }
            this.confCtx.onCaughtUp(version, peer, false);
        } finally {
            writeLock.unlock();
        }
    }

    private void checkDeadNodes(Configuration conf, long nowMs) {
        final List<PeerId> peers = conf.listPeers();
        int aliveCount = 0;
        final Configuration deadNodes = new Configuration();
        for (final PeerId peer : peers) {
            if (peer.equals(this.serverId)) {
                aliveCount++;
                continue;
            }
            this.checkReplicator(peer);
            if (nowMs - replicatorGroup.getLastRpcSendTimestamp(peer) <= options.getElectionTimeoutMs()) {
                aliveCount++;
                continue;
            }
            deadNodes.addPeer(peer);
        }

        if (aliveCount >= peers.size() / 2 + 1) {
            return;
        }
        LOG.warn("Node {} term {} steps down when alive nodes don't satisfy quorum dead nodes: {} conf: {}",
            getNodeId(), this.currTerm, deadNodes, conf);
        final Status status = new Status();
        status.setError(RaftError.ERAFTTIMEDOUT, "Majority of the group dies: %d/%d", deadNodes.size(), peers.size());
        stepDown(this.currTerm, false, status);
    }

    private void handleStepDownTimeout() {
        writeLock.lock();
        try {
            if (state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.debug("Node {} term {} stop stepdown timer state is {}", getNodeId(), this.currTerm, this.state);
                return;
            }
            final long now = Utils.nowMs();
            checkDeadNodes(this.conf.getConf(), now);
            if (!conf.getOldConf().isEmpty()) {
                checkDeadNodes(conf.getOldConf(), now);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Configuration changed callback.
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-11 2:53:43 PM
     */
    private class ConfigurationChangeDone implements Closure {
        private final long    term;
        private final boolean leaderStart;

        public ConfigurationChangeDone(long term, boolean leaderStart) {
            super();
            this.term = term;
            this.leaderStart = leaderStart;
        }

        @Override
        public void run(Status status) {
            if (status.isOk()) {
                onConfigurationChangeDone(term);
                if (leaderStart) {
                    getOptions().getFsm().onLeaderStart(term);
                }
            } else {
                LOG.error("Fail to run ConfigurationChangeDone, status : {}", status);
            }

        }

    }

    private void unsafeApplyConfiguration(Configuration newConf, Configuration oldConf, boolean leaderStart) {
        Requires.requireTrue(this.confCtx.isBusy(), "ConfigurationContext is not busy");
        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.setId(new LogId(0, this.currTerm));
        entry.setPeers(newConf.listPeers());
        if (oldConf != null) {
            entry.setOldPeers(oldConf.listPeers());
        }
        final ConfigurationChangeDone configurationChangeDone = new ConfigurationChangeDone(this.currTerm, leaderStart);
        // Use the new_conf to deal the quorum of this very log
        if (!this.ballotBox.appendPendingTask(newConf, oldConf, configurationChangeDone)) {
            Utils.runClosureInThread(configurationChangeDone, new Status(RaftError.EINTERNAL, "Fail to append task."));
            return;
        }
        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);
        logManager.appendEntries(entries, new LeaderStableClosure(entries));
        this.conf = this.logManager.checkAndSetConfiguration(this.conf);
    }

    private void unsafeRegisterConfChange(Configuration oldConf, Configuration newConf, Closure done) {
        if (state != State.STATE_LEADER) {
            LOG.warn("Node {} refushed configuration changing as the state is {}", getNodeId(), this.state);
            if (done != null) {
                final Status status = new Status();
                if (state == State.STATE_TRANSFERRING) {
                    status.setError(RaftError.EBUSY, "Is transferring leadership.");
                } else {
                    status.setError(RaftError.EPERM, "Not leader");
                }
                Utils.runClosureInThread(done, status);
            }
            return;
        }
        // check concurrent conf change
        if (confCtx.isBusy()) {
            LOG.warn("Node [] refushed configuration concurrent changing.", getNodeId());
            if (done != null) {
                final Status status = new Status(RaftError.EBUSY, "Doing another configuration change.");
                Utils.runClosureInThread(done, status);
            }
            return;
        }
        // Return immediately when the new peers equals to current configuration
        if (this.conf.getConf().equals(newConf)) {
            Utils.runClosureInThread(done);
            return;
        }
        confCtx.start(oldConf, newConf, done);
    }

    private void afterShutdown() {
        List<Closure> savedDones = null;
        writeLock.lock();
        try {
            if (this.shutdownContinuations != null) {
                savedDones = new ArrayList<>(this.shutdownContinuations);
            }
            if (logStorage != null) {
                this.logStorage.shutdown();
            }
            this.state = State.STATE_SHUTDOWN;
        } finally {
            writeLock.unlock();
        }
        if (savedDones != null) {
            for (final Closure closure : savedDones) {
                if (closure == null) {
                    continue;
                }
                Utils.runClosureInThread(closure);
            }
        }
    }

    @Override
    public NodeOptions getOptions() {
        return this.options;
    }

    public TimerManager getTimerManager() {
        return this.timerManager;
    }

    @Override
    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    @OnlyForTest
    long getCurrentTerm() {
        this.readLock.lock();
        try {
            return this.currTerm;
        } finally {
            readLock.unlock();
        }
    }

    @OnlyForTest
    ConfigurationEntry getConf() {
        this.readLock.lock();
        try {
            return this.conf;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.shutdown(null);
    }

    public void onConfigurationChangeDone(long term) {
        writeLock.lock();
        try {
            if (state.compareTo(State.STATE_TRANSFERRING) > 0 || term != currTerm) {
                LOG.warn("Node {} process onConfigurationChangeDone at term={} while state={} and currTerm={}",
                    this.getNodeId(), term, this.state, this.currTerm);
                return;
            }
            this.confCtx.nextStage();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public PeerId getLeaderId() {
        readLock.lock();
        try {
            if (this.leaderId.isEmpty()) {
                return null;
            }
            return this.leaderId;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public String getGroupId() {
        return this.groupId;
    }

    public PeerId getServerId() {
        return this.serverId;
    }

    @Override
    public NodeId getNodeId() {
        if (nodeId == null) {
            nodeId = new NodeId(this.groupId, this.serverId);
        }
        return nodeId;
    }

    public RaftClientService getRpcService() {
        return this.rpcService;
    }

    public void onError(RaftException error) {
        LOG.warn("Node {} got error={}", getNodeId(), error);
        if (fsmCaller != null) {
            // onError of fsmCaller is guaranteed to be executed once.
            fsmCaller.onError(error);
        }
        writeLock.lock();
        try {
            // if it is leader, need to wake up a new one.
            // if it is follower, also step down to call on_stop_following
            if (state.compareTo(State.STATE_FOLLOWER) <= 0) {
                stepDown(currTerm, state == State.STATE_LEADER,
                        new Status(RaftError.EBADNODE, "Raft node(leader or candidate) is in error."));
            }
            if (state.compareTo(State.STATE_ERROR) < 0) {
                state = State.STATE_ERROR;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void handleRequestVoteResponse(PeerId peerId, long term, RequestVoteResponse response) {
        writeLock.lock();
        try {
            if (state != State.STATE_CANDIDATE) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {} state not in STATE_CANDIDATE but {}",
                    getNodeId(), peerId, this.state);
                return;
            }
            //check stale term
            if (term != this.currTerm) {
                LOG.warn("Node {} received stale RequestVoteResponse from {}  term {} currTerm {}", getNodeId(), peerId,
                    term, this.currTerm);
                return;
            }
            //check response term
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}  term {} expect {}", getNodeId(), peerId,
                    response.getTerm(), this.currTerm);
                stepDown(response.getTerm(), false,
                    new Status(RaftError.EHIGHERTERMRESPONSE, "Raft node receives higher term request_vote_response."));
                return;
            }
            // check granted quorum?
            if (response.getGranted()) {
                this.voteCtx.grant(peerId);
                if (this.voteCtx.isGranted()) {
                    becomeLeader();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    private class OnRequestVoteRpcDone extends RpcResponseClosureAdapter<RequestVoteResponse> {

        public OnRequestVoteRpcDone(PeerId peer, long term, NodeImpl node) {
            super();
            this.startMs = Utils.monotonicMs();
            this.peer = peer;
            this.term = term;
            this.node = node;
        }

        @Override
        public void run(Status status) {
            metrics.recordLatency("request-vote", Utils.monotonicMs() - this.startMs);
            if (!status.isOk()) {
                LOG.warn("Node {} RequestVote to {} error: {}", this.node.getNodeId(), peer, status);
            } else {
                node.handleRequestVoteResponse(peer, term, getResponse());
            }
        }

        long               startMs;
        PeerId             peer;
        long               term;
        RequestVoteRequest request;
        NodeImpl           node;
    }

    public void handlePreVoteResponse(PeerId peerId, long term, RequestVoteResponse response) {
        boolean doUnlock = true;
        writeLock.lock();
        try {
            if (state != State.STATE_FOLLOWER) {
                LOG.warn("Node {} received invalid PreVoteResponse from {} state not in STATE_FOLLOWER but {}",
                    this.getNodeId(), peerId, this.state);
                return;
            }
            if (term != this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {} term {} currTerm {}", this.getNodeId(),
                    peerId, term, this.currTerm);
                return;
            }
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {} term {} expect {}", this.getNodeId(), peerId,
                    response.getTerm(), this.currTerm);
                stepDown(response.getTerm(), false,
                    new Status(RaftError.EHIGHERTERMRESPONSE, "Raft node receives higher term pre_vote_response."));
                return;
            }
            LOG.info("Node {} received PreVoteResponse from {} term {} granted {}", this.getNodeId(), peerId,
                response.getTerm(), response.getGranted());
            // check granted quorum?
            if (response.getGranted()) {
                prevVoteCtx.grant(peerId);
                if (prevVoteCtx.isGranted()) {
                    doUnlock = false;
                    electSelf();
                }
            }
        } finally {
            if (doUnlock) {
                writeLock.unlock();
            }
        }
    }

    private class OnPreVoteRpcDone extends RpcResponseClosureAdapter<RequestVoteResponse> {

        private final long startMs;

        public OnPreVoteRpcDone(PeerId peer, long term) {
            super();
            this.startMs = Utils.monotonicMs();
            this.peer = peer;
            this.term = term;
        }

        @Override
        public void run(Status status) {
            metrics.recordLatency("pre-vote", Utils.monotonicMs() - this.startMs);
            if (!status.isOk()) {
                LOG.warn("Node {} PreVote to {} error: {}", getNodeId(), peer, status);
            } else {
                handlePreVoteResponse(peer, term, getResponse());
            }
        }

        PeerId             peer;
        long               term;
        RequestVoteRequest request;
    }

    // in writeLock
    private void preVote() {
        long oldTerm;
        try {
            LOG.info("Node {} term {} start preVote", this.getNodeId(), this.currTerm);
            if (this.snapshotExecutor != null && snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn(
                    "Node {} term {} doesn't do preVote when installing snapshot as the configuration may be out of date.",
                    getNodeId());
                return;
            }
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do preVote as it is not in conf <{}>", this.getNodeId(), this.conf.getConf());
                return;
            }
            oldTerm = this.currTerm;
        } finally {
            writeLock.unlock();
        }

        final LogId lastLogId = this.logManager.getLastLogId(true);

        boolean doUnlock = true;
        writeLock.lock();
        try {
            // pre_vote need defense ABA after unlock&writeLock
            if (oldTerm != currTerm) {
                LOG.warn("Node {} raise term {} when get lastLogId", this.getNodeId(), this.currTerm);
                return;
            }
            this.prevVoteCtx.init(conf.getConf(), conf.isStable() ? null : conf.getOldConf());
            for (final PeerId peer : this.conf.listPeers()) {
                if (peer.equals(this.serverId)) {
                    continue;
                }
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, addr: {}", this.getNodeId(), peer.getEndpoint());
                    continue;
                }
                final OnPreVoteRpcDone done = new OnPreVoteRpcDone(peer, currTerm);
                final RequestVoteRequest.Builder reqBuilder = RequestVoteRequest.newBuilder();
                reqBuilder.setPreVote(true); //It's a pre-vote request.
                reqBuilder.setGroupId(this.groupId);
                reqBuilder.setServerId(this.serverId.toString());
                reqBuilder.setPeerId(peer.toString());
                reqBuilder.setTerm(currTerm + 1); //next term
                reqBuilder.setLastLogIndex(lastLogId.getIndex());
                reqBuilder.setLastLogTerm(lastLogId.getTerm());
                done.request = reqBuilder.build();
                rpcService.preVote(peer.getEndpoint(), done.request, done);
            }
            prevVoteCtx.grant(this.serverId);
            if (prevVoteCtx.isGranted()) {
                doUnlock = false;
                electSelf();
            }
        } finally {
            if (doUnlock) {
                writeLock.unlock();
            }
        }

    }

    private void handleVoteTimeout() {
        writeLock.lock();
        if (state == State.STATE_CANDIDATE) {
            LOG.debug("Node {} term {} retry elect", this.getNodeId(), this.currTerm);
            electSelf();
        } else {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isLeader() {
        readLock.lock();
        try {
            return this.state == State.STATE_LEADER;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void shutdown(Closure done) {
        writeLock.lock();
        try {
            LOG.info("Node {} shutdown, currTerm {} state {}", getNodeId(), this.currTerm, this.state);
            if (state.compareTo(State.STATE_SHUTTING) < 0) {
                NodeManager.getInstance().remove(this);
                // if it is leader, set the wakeup_a_candidate with true,
                // if it is follower, call on_stop_following in step_down
                if (state.compareTo(State.STATE_FOLLOWER) <= 0) {
                    stepDown(currTerm, state == State.STATE_LEADER,
                            new Status(RaftError.ESHUTDOWN, "Raft node is going to quit."));
                }
                this.state = State.STATE_SHUTTING;
                //Destroy all timers
                if (this.electionTimer != null) {
                    this.electionTimer.destroy();
                }
                if (this.voteTimer != null) {
                    this.voteTimer.destroy();
                }
                if (this.stepDownTimer != null) {
                    this.stepDownTimer.destroy();
                }
                if (this.snapshotTimer != null) {
                    this.snapshotTimer.destroy();
                }
                if (this.readOnlyService != null) {
                    this.readOnlyService.shutdown();
                }
                if (logManager != null) {
                    logManager.shutdown();
                }
                if (metaStorage != null) {
                    metaStorage.shutdown();
                }
                if (this.snapshotExecutor != null) {
                    this.snapshotExecutor.shutdown();
                }
                if (this.wakingCandidate != null) {
                    Replicator.stop(this.wakingCandidate);
                }
                if (fsmCaller != null) {
                    fsmCaller.shutdown();
                }
                if (this.rpcService != null) {
                    this.rpcService.shutdown();
                }

                if (this.applyQueue != null) {
                    this.shutdownLatch = new CountDownLatch(1);
                    this.applyQueue.publishEvent((event, sequence) -> event.shutdownLatch = this.shutdownLatch);
                } else {
                    GLOBAL_NUM_NODES.decrementAndGet();
                }
                if (this.timerManager != null) {
                    this.timerManager.shutdown();
                }
            }

            if (state != State.STATE_SHUTDOWN) {
                if (done != null) {
                    this.shutdownContinuations.add(done);
                }
                return;
            }
            // This node is down, it's ok to invoke done right now. Don't invoke this
            // in place to avoid the dead writeLock issue when done.Run() is going to acquire
            // a writeLock which is already held by the caller
            if (done != null) {
                Utils.runClosureInThread(done);
            }

        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public synchronized void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            if (readOnlyService != null) {
                readOnlyService.join();
            }
            if (fsmCaller != null) {
                fsmCaller.join();
            }
            if (logManager != null) {
                logManager.join();
            }
            if (this.snapshotExecutor != null) {
                this.snapshotExecutor.join();
            }
            if (this.wakingCandidate != null) {
                Replicator.join(this.wakingCandidate);
            }
            this.shutdownLatch.await();
            this.applyDisruptor.shutdown();
            this.shutdownLatch = null;
        }
    }

    private static class StopTransferArg {
        NodeImpl node;
        long     term;
        PeerId   peer;

        public StopTransferArg(NodeImpl node, long term, PeerId peer) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
        }

    }

    private void handleTransferTimeout(long term, PeerId peer) {
        LOG.info("Node {} failed to transfer leadership to peer={} : reached timeout", this.getNodeId(), peer);
        writeLock.lock();
        try {
            if (term == currTerm) {
                replicatorGroup.stopTransferLeadership(peer);
                if (state == State.STATE_TRANSFERRING) {
                    fsmCaller.onLeaderStart(term);
                    state = State.STATE_LEADER;
                    this.stopTransferArg = null;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void onTransferTimeout(StopTransferArg arg) {
        arg.node.handleTransferTimeout(arg.term, arg.peer);
    }

    /**
     * Retrieve current configuration this node seen so far. It's not a reliable way to
     * retrieve cluster peers info, you should use {@link #listPeers()} instead.
     * @since 1.0.3
     * @return returns current configuration.
     */
    public Configuration getCurrentConf() {
        readLock.lock();
        try {
            if (this.conf != null && this.conf.getConf() != null) {
                return this.conf.getConf().copy();
            }
            return null;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listPeers() {
        readLock.lock();
        try {
            if (state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return conf.getConf().listPeers();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addPeer(PeerId peer, Closure done) {
        Requires.requireNonNull(peer, "Null peer");
        writeLock.lock();
        try {
            Requires.requireTrue(!this.conf.getConf().contains(peer), "Peer already exists in current configuration.");

            final Configuration newConf = new Configuration(conf.getConf());
            newConf.addPeer(peer);
            unsafeRegisterConfChange(conf.getConf(), newConf, done);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removePeer(PeerId peer, Closure done) {
        Requires.requireNonNull(peer, "Null peer");
        writeLock.lock();
        try {
            Requires.requireTrue(this.conf.getConf().contains(peer), "Peer not found in current configuration.");

            final Configuration newConf = new Configuration(conf.getConf());
            newConf.removePeer(peer);
            unsafeRegisterConfChange(conf.getConf(), newConf, done);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void changePeers(Configuration newPeers, Closure done) {
        Requires.requireNonNull(newPeers, "Null new peers");
        Requires.requireTrue(!newPeers.isEmpty(), "Empty new peers");
        writeLock.lock();
        try {
            LOG.info("Node {} change peers from {} to {}", getNodeId(), this.conf.getConf(), newPeers);
            unsafeRegisterConfChange(conf.getConf(), newPeers, done);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Status resetPeers(Configuration newPeers) {
        Requires.requireNonNull(newPeers, "Null new peers");
        Requires.requireTrue(!newPeers.isEmpty(), "Empty new peers");
        writeLock.lock();
        try {
            if (newPeers.isEmpty()) {
                LOG.warn("Node {} set empty peers", this.getNodeId());
                return new Status(RaftError.EINVAL, "newPeers is empty");
            }
            if (!state.isActive()) {
                LOG.warn("Node {} is in state {}, can't set peers", getNodeId(), this.state);
                return new Status(RaftError.EPERM, "Bad state: %s", state);
            }
            //bootstrap?
            if (conf.getConf().isEmpty()) {
                LOG.info("Node {} set peers to {} from empty", this.getNodeId(), newPeers);
                this.conf.setConf(newPeers);
                final Status st = new Status(RaftError.ESETPEER, "Set peer from empty configuration");
                this.stepDown(currTerm + 1, false, st);
                return Status.OK();
            }
            if (state == State.STATE_LEADER && confCtx.isBusy()) {
                LOG.warn("Node {} set peers need wait current conf changing", this.getNodeId());
                return new Status(RaftError.EBUSY, "Changing to another configuration");
            }
            // check equal, maybe retry direct return
            if (conf.getConf().equals(newPeers)) {
                return Status.OK();
            }
            final Configuration newConf = new Configuration(newPeers);
            LOG.info("Node {} set peers from {} to {}", getNodeId(), this.conf.getConf(), newPeers);
            this.conf.setConf(newConf);
            this.conf.getOldConf().reset();
            this.stepDown(currTerm + 1, false, new Status(RaftError.ESETPEER, "Raft node set peer normally"));
            return Status.OK();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void snapshot(Closure done) {
        this.doSnapshot(done);
    }

    private void doSnapshot(Closure done) {
        if (this.snapshotExecutor != null) {
            snapshotExecutor.doSnapshot(done);
        } else {
            if (done != null) {
                final Status status = new Status(RaftError.EINVAL, "Snapshot is not supported");
                Utils.runClosureInThread(done, status);
            }
        }
    }

    @Override
    public void resetElectionTimeoutMs(int electionTimeoutMs) {
        if (electionTimeoutMs <= 0) {
            throw new IllegalArgumentException("Invalid value");
        }
        writeLock.lock();
        try {
            options.setElectionTimeoutMs(electionTimeoutMs);
            this.replicatorGroup.resetHeartbeatInterval(heartbeatTimeout(options.getElectionTimeoutMs()));
            this.replicatorGroup.resetElectionTimeoutInterval(electionTimeoutMs);
            LOG.info("Node {} reset electionTimeout, currTimer {} state {} new electionTimeout {}", getNodeId(),
                this.currTerm, this.state, electionTimeoutMs);
            this.electionTimer.reset(electionTimeoutMs);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Status transferLeadershipTo(PeerId peer) {
        Requires.requireNonNull(peer, "Null peer");
        writeLock.lock();
        try {
            if (state != State.STATE_LEADER) {
                LOG.warn("Node {} can't transfer leadership to peer {} as it is in state {}", this.getNodeId(), peer,
                    this.state);
                return new Status(state == State.STATE_TRANSFERRING ? RaftError.EBUSY : RaftError.EPERM,
                        "Not a leader");
            }
            if (confCtx.isBusy()) {
                // It's very messy to deal with the case when the |peer| received
                // TimeoutNowRequest and increase the term while somehow another leader
                // which was not replicated with the newest configuration has been
                // elected. If no add_peer with this very |peer| is to be invoked ever
                // after nor this peer is to be killed, this peer will spin in the voting
                // procedure and make the each new leader stepped down when the peer
                // reached vote timedout and it starts to vote (because it will increase
                // the term of the group)
                // To make things simple, refuse the operation and force users to
                // invoke transfer_leadership_to after configuration changing is
                // completed so that the peer's configuration is up-to-date when it
                // receives the TimeOutNowRequest.
                LOG.warn(
                    "Node {} refused to transfer leadership to peer {} when the leader is changing the configuration",
                    this.getNodeId(), peer);
                return new Status(RaftError.EBUSY, "Changing the configuration");
            }

            PeerId peerId = peer.copy();
            // if peer_id is ANY_PEER(0.0.0.0:0:0), the peer with the largest
            // last_log_id will be selected.
            if (peerId.equals(PeerId.ANY_PEER)) {
                LOG.info("Node {} starts to transfer leadership to any peer.", getNodeId());
                if ((peerId = replicatorGroup.findTheNextCandidate(this.conf)) == null) {
                    return new Status(-1, "Candidate not found for any peer");
                }
            }
            if (peerId.equals(this.serverId)) {
                LOG.info("Node {} transfered leadership to self.");
                return Status.OK();
            }
            if (!conf.contains(peerId)) {
                LOG.info("Node {} refused to transfer leadership to peer {} as it is not in {}", getNodeId(), peer,
                    this.conf.getConf());
                return new Status(RaftError.EINVAL, "Not in current configuration");
            }

            final long lastLogIndex = logManager.getLastLogIndex();
            if (!this.replicatorGroup.transferLeadershipTo(peerId, lastLogIndex)) {
                LOG.warn("No such peer {}", peer);
                return new Status(RaftError.EINVAL, "No such peer %s", peer);
            }
            this.state = State.STATE_TRANSFERRING;
            final Status status = new Status(RaftError.ETRANSFERLEADERSHIP,
                "Raft leader is transferring leadership to %s", peerId);
            onLeaderStop(status);
            LOG.info("Node {} starts to transfer leadership to peer {}", getNodeId(), peer);
            final StopTransferArg stopArg = new StopTransferArg(this, currTerm, peerId);
            this.stopTransferArg = stopArg;
            this.transferTimer = this.timerManager.schedule(() -> onTransferTimeout(stopArg),
                this.options.getElectionTimeoutMs(), TimeUnit.MILLISECONDS);

        } finally {
            writeLock.unlock();
        }
        return Status.OK();
    }

    private void onLeaderStop(Status status) {
        this.replicatorGroup.clearFailureReplicators();
        fsmCaller.onLeaderStop(status);
    }

    @Override
    public Message handleTimeoutNowRequest(TimeoutNowRequest request, RpcRequestClosure done) {
        final TimeoutNowResponse.Builder rb = TimeoutNowResponse.newBuilder();
        boolean doUnlock = true;
        writeLock.lock();
        try {
            if (request.getTerm() != this.currTerm) {
                final long savedCurrTerm = this.currTerm;
                if (request.getTerm() > this.currTerm) {
                    this.stepDown(request.getTerm(), false,
                        new Status(RaftError.EHIGHERTERMREQUEST, "Raft node receives higher term request"));
                }
                rb.setTerm(this.currTerm);
                rb.setSuccess(false);
                LOG.info("Node {} received TimeoutNowRequest from {} while currTerm={} didn't match requestTerm={}",
                    this.getNodeId(), request.getPeerId(), savedCurrTerm, request.getTerm());
                return rb.build();
            }
            if (state != State.STATE_FOLLOWER) {
                LOG.info("Node {} received TimeoutNowRequest from {} while state is {} at term={}", getNodeId(),
                    request.getServerId(), this.state, this.currTerm);
                rb.setTerm(this.currTerm);
                rb.setSuccess(false);
                return rb.build();
            }

            final long savedTerm = this.currTerm;
            rb.setTerm(this.currTerm + 1);
            rb.setSuccess(true);
            // Parallelize Response and election
            done.sendResponse(rb.build());
            doUnlock = false;
            this.electSelf();
            LOG.info("Node {} received TimeoutNowRequest from {} at term={}", this.getNodeId(), request.getServerId(),
                savedTerm);
        } finally {
            if (doUnlock) {
                writeLock.unlock();
            }
        }
        return null;
    }

    @Override
    public Message handleInstallSnapshot(InstallSnapshotRequest request, RpcRequestClosure done) {
        if (this.snapshotExecutor == null) {
            return RpcResponseFactory.newResponse(RaftError.EINVAL, "Not supported snapshot");
        }
        final PeerId serverId = new PeerId();
        if (!serverId.parse(request.getServerId())) {
            LOG.warn("Node {} ignore InstallSnapshotRequest from {} bad server id", getNodeId(), request.getServerId());
            return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse serverId failed: %s", request.getServerId());
        }

        final InstallSnapshotResponse.Builder responseBuilder = InstallSnapshotResponse.newBuilder();
        writeLock.lock();
        try {
            if (!state.isActive()) {
                LOG.warn("Node {} ignore InstallSnapshotRequest as it is not in active state {}", getNodeId(),
                    this.state);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s:%s is not in active state, state %s.",
                    this.groupId, this.serverId, this.state.name());
            }

            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale InstallSnapshotRequest from {} in term {} currTerm {}", getNodeId(),
                    request.getPeerId(), request.getTerm(), this.currTerm);
                responseBuilder.setTerm(this.currTerm);
                responseBuilder.setSuccess(false);
                return responseBuilder.build();
            }

            checkStepDown(request.getTerm(), serverId);
            if (!serverId.equals(leaderId)) {
                LOG.error("Another peer={} declares that it is the leader at term={} which was occupied by leader={}",
                    serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                this.stepDown(request.getTerm() + 1, false,
                    new Status(RaftError.ELEADERCONFLICT, "More than one leader in the same term."));
                responseBuilder.setSuccess(false);
                responseBuilder.setTerm(request.getTerm() + 1);
                return responseBuilder.build();
            }

        } finally {
            writeLock.unlock();
        }
        final long startMs = Utils.monotonicMs();
        try {
            LOG.info(
                "Node {} received InstallSnapshotRequest lastIncludedLogIndex {} lastIncludedLogTerm {} from {} when lastLogId={}",
                getNodeId(), request.getMeta().getLastIncludedIndex(), request.getMeta().getLastIncludedTerm(),
                request.getServerId(), logManager.getLastLogId(false));
            this.snapshotExecutor.installSnapshot(request, responseBuilder, done);
            return null;
        } finally {
            this.metrics.recordLatency("install-snapshot", Utils.monotonicMs() - startMs);
        }
    }

    public void updateConfigurationAfterInstallingSnapshot() {
        writeLock.lock();
        try {
            this.conf = this.logManager.checkAndSetConfiguration(this.conf);
        } finally {
            writeLock.unlock();
        }
    }

    private void stopReplicator(List<PeerId> keep, List<PeerId> drop) {
        if (drop != null) {
            for (final PeerId peer : drop) {
                if (!keep.contains(peer) && !peer.equals(this.serverId)) {
                    this.replicatorGroup.stopReplicator(peer);
                }
            }
        }
    }

    @Override
    public UserLog readCommittedUserLog(long index) {
        if (index <= 0) {
            throw new LogIndexOutOfBoundsException("request index is invalid: " + index);
        }

        final long savedLastAppliedIndex = fsmCaller.getLastAppliedIndex();

        if (index > savedLastAppliedIndex) {
            throw new LogIndexOutOfBoundsException(
                "request index " + index + " is greater than lastAppliedIndex: " + savedLastAppliedIndex);
        }

        long curIndex = index;
        LogEntry entry = this.logManager.getEntry(curIndex);
        if (entry == null) {
            throw new LogNotFoundException("user log is deleted at index: " + index);
        }

        do {
            if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA) {
                return new UserLog(curIndex, entry.getData());
            } else {
                curIndex++;
            }
            if (curIndex > savedLastAppliedIndex) {
                throw new IllegalStateException(
                    "No user log between index:" + index + " and last_applied_index:" + savedLastAppliedIndex);
            }
            entry = logManager.getEntry(curIndex);
        } while (entry != null);

        throw new LogNotFoundException("user log is deleted at index: " + curIndex);
    }

    @Override
    public String toString() {
        return "JRaftNode [nodeId=" + this.getNodeId() + "]";
    }

}
