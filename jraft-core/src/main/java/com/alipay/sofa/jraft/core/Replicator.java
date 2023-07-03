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
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.core.Replicator.ReplicatorStateListener.ReplicatorState;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReplicatorOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.rpc.RpcUtils;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.util.BufferUtils;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Recyclable;
import com.alipay.sofa.jraft.util.RecyclableByteBufferList;
import com.alipay.sofa.jraft.util.RecycleUtil;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.ThreadPoolsFactory;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Replicator for replicating log entry from leader to followers.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 10:32:02 AM
 */
@ThreadSafe
public class Replicator implements ThreadId.OnError {

    private static final Logger              LOG                    = LoggerFactory.getLogger(Replicator.class);

    private final RaftClientService          rpcService;
    // Next sending log index
    private volatile long                    nextIndex;
    private int                              consecutiveErrorTimes  = 0;
    private boolean                          hasSucceeded;
    private long                             timeoutNowIndex;
    private volatile long                    lastRpcSendTimestamp;
    private volatile long                    heartbeatCounter       = 0;
    private volatile long                    probeCounter           = 0;
    private volatile long                    appendEntriesCounter   = 0;
    private volatile long                    installSnapshotCounter = 0;
    private volatile long                    blockCounter           = 0;
    protected Stat                           statInfo               = new Stat();
    private ScheduledFuture<?>               blockTimer;

    // Cached the latest RPC in-flight request.
    private Inflight                         rpcInFly;
    // Heartbeat RPC future
    private Future<Message>                  heartbeatInFly;
    // Timeout request RPC future
    private Future<Message>                  timeoutNowInFly;
    // In-flight RPC requests, FIFO queue
    private final ArrayDeque<Inflight>       inflights              = new ArrayDeque<>();

    private long                             waitId                 = -1L;
    protected ThreadId                       id;
    private final ReplicatorOptions          options;
    private final RaftOptions                raftOptions;

    private ScheduledFuture<?>               heartbeatTimer;
    private volatile SnapshotReader          reader;
    private CatchUpClosure                   catchUpClosure;
    private final Scheduler                  timerManager;
    private final NodeMetrics                nodeMetrics;
    private volatile State                   state;

    // Request sequence
    private int                              reqSeq                 = 0;
    // Response sequence
    private int                              requiredNextSeq        = 0;
    // Replicator state reset version
    private int                              version                = 0;

    // Pending response queue;
    private final PriorityQueue<RpcResponse> pendingResponses       = new PriorityQueue<>(50);

    private final String metricName;

    private int getAndIncrementReqSeq() {
        final int prev = this.reqSeq;
        this.reqSeq++;
        if (this.reqSeq < 0) {
            this.reqSeq = 0;
        }
        return prev;
    }

    private int getAndIncrementRequiredNextSeq() {
        final int prev = this.requiredNextSeq;
        this.requiredNextSeq++;
        if (this.requiredNextSeq < 0) {
            this.requiredNextSeq = 0;
        }
        return prev;
    }

    /**
     * Replicator internal state
     * @author dennis
     *
     */
    public enum State {
        Created,
        Probe, // probe follower state
        Snapshot, // installing snapshot to follower
        Replicate, // replicate logs normally
        Destroyed // destroyed
    }

    public Replicator(final ReplicatorOptions replicatorOptions, final RaftOptions raftOptions) {
        super();
        this.options = replicatorOptions;
        this.nodeMetrics = this.options.getNode().getNodeMetrics();
        this.nextIndex = this.options.getLogManager().getLastLogIndex() + 1;
        this.timerManager = replicatorOptions.getTimerManager();
        this.raftOptions = raftOptions;
        this.rpcService = replicatorOptions.getRaftRpcService();
        this.metricName = getReplicatorMetricName(replicatorOptions);
        setState(State.Created);
    }

    /**
     * Replicator metric set.
     * @author dennis
     *
     */
    private static final class ReplicatorMetricSet implements MetricSet {
        private final ReplicatorOptions opts;
        private final Replicator        r;

        private ReplicatorMetricSet(final ReplicatorOptions opts, final Replicator r) {
            this.opts = opts;
            this.r = r;
        }

        @Override
        public Map<String, Metric> getMetrics() {
            final Map<String, Metric> gauges = new HashMap<>();
            gauges.put("log-lags",
                (Gauge<Long>) () -> this.opts.getLogManager().getLastLogIndex() - (this.r.nextIndex - 1));
            gauges.put("next-index", (Gauge<Long>) () -> this.r.nextIndex);
            gauges.put("heartbeat-times", (Gauge<Long>) () -> this.r.heartbeatCounter);
            gauges.put("install-snapshot-times", (Gauge<Long>) () -> this.r.installSnapshotCounter);
            gauges.put("probe-times", (Gauge<Long>) () -> this.r.probeCounter);
            gauges.put("block-times", (Gauge<Long>) () -> this.r.blockCounter);
            gauges.put("append-entries-times", (Gauge<Long>) () -> this.r.appendEntriesCounter);
            gauges.put("consecutive-error-times", (Gauge<Long>) () -> (long) this.r.consecutiveErrorTimes);
            gauges.put("state", (Gauge<Long>) () -> (long) this.r.state.ordinal());
            gauges.put("running-state", (Gauge<Long>) () -> (long) this.r.statInfo.runningState.ordinal());
            gauges.put("locked", (Gauge<Long>) () ->  (null == this.r.id ? -1L : this.r.id.isLocked() ? 1L : 0L));
            return gauges;
        }
    }

    /**
     * Internal state
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 10:38:45 AM
     */
    enum RunningState {
        IDLE, // idle
        BLOCKING, // blocking state
        APPENDING_ENTRIES, // appending log entries
        INSTALLING_SNAPSHOT // installing snapshot
    }

    enum ReplicatorEvent {
        CREATED, // created
        ERROR, // error
        DESTROYED ,// destroyed
        STATE_CHANGED; // state changed.
    }

    /**
     * User can implement the ReplicatorStateListener interface by themselves.
     * So they can do some their own logic codes when replicator created, destroyed or had some errors.
     *
     * @author zongtanghu
     *
     * 2019-Aug-20 2:32:10 PM
     */
    public interface ReplicatorStateListener {

      /**
       * Represents state changes in the replicator.
       * @author boyan(boyan@antfin.com)
       *
       */
      enum ReplicatorState {
        /**
         * The replicator is created.
         */
        CREATED,
        /**
         * The replicator is destroyed.
         */
        DESTROYED,
        /**
         * The replicator begins to doing it's job(replicating logs or installing snapshot).
         */
        ONLINE,
        /**
         * The replicaotr is suspended by raft error or lost connection.
         */
        OFFLINE
      }


        /**
         * Called when this replicator has been created.
         *
         * @param peer   replicator related peerId
         */
        void onCreated(final PeerId peer);

        /**
         * Called when this replicator has some errors.
         *
         * @param peer   replicator related peerId
         * @param status replicator's error detailed status
         */
        void onError(final PeerId peer, final Status status);

        /**
         * Called when this replicator has been destroyed.
         *
         * @param peer   replicator related peerId
         */
        void onDestroyed(final PeerId peer);

        /**
         * Called when the replicator state is changed. See {@link ReplicatorState}
         * @param peer the replicator's peer id.
         * @param newState the new replicator state.
         * @since 1.3.6
         */
        default void stateChanged(final PeerId peer, final ReplicatorState newState) {}
    }

    private static void notifyReplicatorStatusListener(final Replicator replicator, final ReplicatorEvent event,
        final Status status) {
      notifyReplicatorStatusListener(replicator, event, status, null);
    }

    /**
     * Notify replicator event(such as created, error, destroyed) to replicatorStateListener which is implemented by users.
     *
     * @param replicator replicator object
     * @param event      replicator's state listener event type
     * @param status     replicator's error detailed status
     */
    private static void notifyReplicatorStatusListener(final Replicator replicator, final ReplicatorEvent event,
                                                       final Status status, final ReplicatorState newState) {
        final ReplicatorOptions replicatorOpts = Requires.requireNonNull(replicator.getOpts(), "replicatorOptions");
        final Node node = Requires.requireNonNull(replicatorOpts.getNode(), "node");
        final PeerId peer = Requires.requireNonNull(replicatorOpts.getPeerId(), "peer");

        final List<ReplicatorStateListener> listenerList = node.getReplicatorStatueListeners();
        for (int i = 0; i < listenerList.size(); i++) {
            final ReplicatorStateListener listener = listenerList.get(i);
            if (listener != null) {
                try {
                    switch (event) {
                        case CREATED:
                            RpcUtils.runInThread(() ->  listener.onCreated(peer));
                            break;
                        case ERROR:
                            RpcUtils.runInThread(() -> listener.onError(peer, status));
                            break;
                        case DESTROYED:
                            RpcUtils.runInThread(() -> listener.onDestroyed(peer));
                            break;
                        case STATE_CHANGED:
                          RpcUtils.runInThread(() ->  listener.stateChanged(peer, newState));
                        default:
                            break;
                    }
                } catch (final Exception e) {
                    LOG.error("Fail to notify ReplicatorStatusListener, listener={}, event={}.", listener, event);
                }
            }
        }
    }

    /**
     * Notify replicator event(such as created, error, destroyed) to replicatorStateListener which is implemented by users for none status.
     *
     * @param replicator replicator object
     * @param event      replicator's state listener event type
     */
    private static void notifyReplicatorStatusListener(final Replicator replicator, final ReplicatorEvent event) {
        notifyReplicatorStatusListener(replicator, event, null);
    }

    /**
     * Statistics structure
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 10:38:53 AM
     */
    static class Stat {
        RunningState runningState;
        long         firstLogIndex;
        long         lastLogIncluded;
        long         lastLogIndex;
        long         lastTermIncluded;

        @Override
        public String toString() {
            return "<running=" + this.runningState + ", firstLogIndex=" + this.firstLogIndex + ", lastLogIncluded="
                   + this.lastLogIncluded + ", lastLogIndex=" + this.lastLogIndex + ", lastTermIncluded="
                   + this.lastTermIncluded + ">";
        }

    }

    // In-flight request type
    enum RequestType {
        Snapshot, // install snapshot
        AppendEntries // replicate logs
    }

    /**
     * In-flight request.
     * @author dennis
     *
     */
    static class Inflight {
        // In-flight request count
        final int             count;
        // Start log index
        final long            startIndex;
        // Entries size in bytes
        final int             size;
        // RPC future
        final Future<Message> rpcFuture;
        final RequestType     requestType;
        // Request sequence.
        final int             seq;

        public Inflight(final RequestType requestType, final long startIndex, final int count, final int size,
                        final int seq, final Future<Message> rpcFuture) {
            super();
            this.seq = seq;
            this.requestType = requestType;
            this.count = count;
            this.startIndex = startIndex;
            this.size = size;
            this.rpcFuture = rpcFuture;
        }

        @Override
        public String toString() {
            return "Inflight [count=" + this.count + ", startIndex=" + this.startIndex + ", size=" + this.size
                   + ", rpcFuture=" + this.rpcFuture + ", requestType=" + this.requestType + ", seq=" + this.seq + "]";
        }

        boolean isSendingLogEntries() {
            return this.requestType == RequestType.AppendEntries && this.count > 0;
        }
    }

    /**
     * RPC response for AppendEntries/InstallSnapshot.
     * @author dennis
     *
     */
    static class RpcResponse implements Comparable<RpcResponse> {
        final Status      status;
        final Message     request;
        final Message     response;
        final long        rpcSendTime;
        final int         seq;
        final RequestType requestType;

        public RpcResponse(final RequestType reqType, final int seq, final Status status, final Message request,
                           final Message response, final long rpcSendTime) {
            super();
            this.requestType = reqType;
            this.seq = seq;
            this.status = status;
            this.request = request;
            this.response = response;
            this.rpcSendTime = rpcSendTime;
        }

        @Override
        public String toString() {
            return "RpcResponse [status=" + this.status + ", request=" + this.request + ", response=" + this.response
                   + ", rpcSendTime=" + this.rpcSendTime + ", seq=" + this.seq + ", requestType=" + this.requestType
                   + "]";
        }

        /**
         * Sort by sequence.
         */
        @Override
        public int compareTo(final RpcResponse o) {
            return Integer.compare(this.seq, o.seq);
        }
    }

    @OnlyForTest
    ArrayDeque<Inflight> getInflights() {
        return this.inflights;
    }

    State getState() {
        return this.state;
    }

    void setState(final State state) {
        State oldState = this.state;
        this.state = state;

        if(oldState != state) {
          ReplicatorState newState = null;
          switch(state) {
            case Created:
              newState = ReplicatorState.CREATED;
              break;
            case Replicate:
            case Snapshot:
              newState = ReplicatorState.ONLINE;
              break;
            case Probe:
              newState = ReplicatorState.OFFLINE;
              break;
            case Destroyed:
              newState = ReplicatorState.DESTROYED;
              break;
          }

          if(newState != null) {
            notifyReplicatorStatusListener(this, ReplicatorEvent.STATE_CHANGED, null, newState);
          }
        }
    }

    @OnlyForTest
    int getReqSeq() {
        return this.reqSeq;
    }

    @OnlyForTest
    int getRequiredNextSeq() {
        return this.requiredNextSeq;
    }

    @OnlyForTest
    int getVersion() {
        return this.version;
    }

    @OnlyForTest
    public PriorityQueue<RpcResponse> getPendingResponses() {
        return this.pendingResponses;
    }

    @OnlyForTest
    long getWaitId() {
        return this.waitId;
    }

    @OnlyForTest
    ScheduledFuture<?> getBlockTimer() {
        return this.blockTimer;
    }

    @OnlyForTest
    long getTimeoutNowIndex() {
        return this.timeoutNowIndex;
    }

    @OnlyForTest
    ReplicatorOptions getOpts() {
        return this.options;
    }

    @OnlyForTest
    long getRealNextIndex() {
        return this.nextIndex;
    }

    @OnlyForTest
    Future<Message> getRpcInFly() {
        if (this.rpcInFly == null) {
            return null;
        }
        return this.rpcInFly.rpcFuture;
    }

    @OnlyForTest
    Future<Message> getHeartbeatInFly() {
        return this.heartbeatInFly;
    }

    @OnlyForTest
    ScheduledFuture<?> getHeartbeatTimer() {
        return this.heartbeatTimer;
    }

    @OnlyForTest
    void setHasSucceeded() {
        this.hasSucceeded = true;
    }

    @OnlyForTest
    Future<Message> getTimeoutNowInFly() {
        return this.timeoutNowInFly;
    }

    /**
     * Adds a in-flight request
     * @param reqType   type of request
     * @param count     count if request
     * @param size      size in bytes
     */
    private void addInflight(final RequestType reqType, final long startIndex, final int count, final int size,
                             final int seq, final Future<Message> rpcInfly) {
        this.rpcInFly = new Inflight(reqType, startIndex, count, size, seq, rpcInfly);
        this.inflights.add(this.rpcInFly);
        this.nodeMetrics.recordSize(name(this.metricName, "replicate-inflights-count"), this.inflights.size());
    }

    /**
     * Returns the next in-flight sending index, return -1 when can't send more in-flight requests.
     *
     * @return next in-flight sending index
     */
    long getNextSendIndex() {
        // Fast path
        if (this.inflights.isEmpty()) {
            return this.nextIndex;
        }
        // Too many in-flight requests.
        if (this.inflights.size() > this.raftOptions.getMaxReplicatorInflightMsgs()) {
            return -1L;
        }
        // Last request should be a AppendEntries request and has some entries.
        if (this.rpcInFly != null && this.rpcInFly.isSendingLogEntries()) {
            return this.rpcInFly.startIndex + this.rpcInFly.count;
        }
        return -1L;
    }

    private Inflight pollInflight() {
        return this.inflights.poll();
    }

    private void startHeartbeatTimer(final long startMs) {
        final long dueTime = startMs + this.options.getDynamicHeartBeatTimeoutMs();
        try {
            this.heartbeatTimer = this.timerManager.schedule(() -> onTimeout(this.id), dueTime - Utils.nowMs(),
                TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOG.error("Replicator {} fail to schedule heartbeat timer ", this, e);
            onTimeout(this.id);
        }
    }

    void installSnapshot() {
        if (getState() == State.Snapshot) {
            LOG.warn("Replicator {} is installing snapshot, ignore the new request.", this);
            unlockId();
            return;
        }
        boolean doUnlock = true;
        if (!this.rpcService.connect(this.options.getPeerId().getEndpoint())) {
            LOG.error("Fail to check install snapshot connection to peer={}, give up to send install snapshot request, group id {}.",
                    this.options.getPeerId().getEndpoint(), this.options.getGroupId());
            block(Utils.nowMs(), RaftError.EHOSTDOWN.getNumber());
            return;
        }
        try {
            Requires.requireTrue(this.reader == null,
                "Replicator [%s, %s, %s] already has a snapshot reader, current state is %s",
                    this.options.getGroupId(), this.options.getPeerId(), this.options.getReplicatorType(), getState());
            this.reader = this.options.getSnapshotStorage().open();
            if (this.reader == null) {
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to open snapshot"));
                unlockId();
                doUnlock = false;
                node.onError(error);
                return;
            }
            final String uri = this.reader.generateURIForCopy();
            if (uri == null) {
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to generate uri for snapshot reader"));
                releaseReader();
                unlockId();
                doUnlock = false;
                node.onError(error);
                return;
            }
            final RaftOutter.SnapshotMeta meta = this.reader.load();
            if (meta == null) {
                final String snapshotPath = this.reader.getPath();
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to load meta from %s", snapshotPath));
                releaseReader();
                unlockId();
                doUnlock = false;
                node.onError(error);
                return;
            }
            final InstallSnapshotRequest.Builder rb = InstallSnapshotRequest.newBuilder();
            rb.setTerm(this.options.getTerm());
            rb.setGroupId(this.options.getGroupId());
            rb.setServerId(this.options.getServerId().toString());
            rb.setPeerId(this.options.getPeerId().toString());
            rb.setMeta(meta);
            rb.setUri(uri);

            this.statInfo.runningState = RunningState.INSTALLING_SNAPSHOT;
            this.statInfo.lastLogIncluded = meta.getLastIncludedIndex();
            this.statInfo.lastTermIncluded = meta.getLastIncludedTerm();

            final InstallSnapshotRequest request = rb.build();
            setState(State.Snapshot);
            // noinspection NonAtomicOperationOnVolatileField
            this.installSnapshotCounter++;
            final long monotonicSendTimeMs = Utils.monotonicMs();
            final int stateVersion = this.version;
            final int seq = getAndIncrementReqSeq();
            final Future<Message> rpcFuture = this.rpcService.installSnapshot(this.options.getPeerId().getEndpoint(),
                request, new RpcResponseClosureAdapter<InstallSnapshotResponse>() {

                    @Override
                    public void run(final Status status) {
                        onRpcReturned(Replicator.this.id, RequestType.Snapshot, status, request, getResponse(), seq,
                            stateVersion, monotonicSendTimeMs);
                    }
                });
            addInflight(RequestType.Snapshot, this.nextIndex, 0, 0, seq, rpcFuture);
        } finally {
            if (doUnlock) {
                unlockId();
            }
        }
    }

    @SuppressWarnings("unused")
    static boolean onInstallSnapshotReturned(final ThreadId id, final Replicator r, final Status status,
                                             final InstallSnapshotRequest request,
                                             final InstallSnapshotResponse response) {
        boolean success = true;
        r.releaseReader();
        // noinspection ConstantConditions
        do {
            final StringBuilder sb = new StringBuilder("Node "). //
                append(r.options.getGroupId()).append(":").append(r.options.getServerId()). //
                append(" received InstallSnapshotResponse from ").append(r.options.getPeerId()). //
                append(" lastIncludedIndex=").append(request.getMeta().getLastIncludedIndex()). //
                append(" lastIncludedTerm=").append(request.getMeta().getLastIncludedTerm());
            if (!status.isOk()) {
                sb.append(" error:").append(status);
                LOG.info(sb.toString());
                notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
                if ((r.consecutiveErrorTimes++) % 10 == 0) {
                    LOG.warn("Fail to install snapshot at peer={}, error={}, group id={}", r.options.getPeerId(), status, r.options.getGroupId());
                }
                success = false;
                break;
            }
            if (!response.getSuccess()) {
                sb.append(" success=false");
                LOG.info(sb.toString());
                success = false;
                break;
            }
            // success
            r.nextIndex = request.getMeta().getLastIncludedIndex() + 1;
            sb.append(" success=true");
            LOG.info(sb.toString());
        } while (false);
        // We don't retry installing the snapshot explicitly.
        // id is unlock in sendEntries
        if (!success) {
            //should reset states
            r.resetInflights();
            r.setState(State.Probe);
            r.block(Utils.nowMs(), status.getCode());
            return false;
        }
        r.hasSucceeded = true;
        r.notifyOnCaughtUp(RaftError.SUCCESS.getNumber(), false);
        if (r.timeoutNowIndex > 0 && r.timeoutNowIndex < r.nextIndex) {
            r.sendTimeoutNow(false, false);
        }
        // id is unlock in _send_entriesheartbeatCounter
        r.setState(State.Replicate);
        return true;
    }

    private void sendEmptyEntries(final boolean isHeartbeat) {
        sendEmptyEntries(isHeartbeat, null);
    }

    /**
     * Send probe or heartbeat request
     * @param isHeartbeat      if current entries is heartbeat
     * @param heartBeatClosure heartbeat callback
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void sendEmptyEntries(final boolean isHeartbeat,
                                  final RpcResponseClosure<AppendEntriesResponse> heartBeatClosure) {
        final AppendEntriesRequest.Builder rb = AppendEntriesRequest.newBuilder();
        if (!fillCommonFields(rb, this.nextIndex - 1, isHeartbeat)) {
            // id is unlock in installSnapshot
            installSnapshot();
            if (isHeartbeat && heartBeatClosure != null) {
                RpcUtils.runClosureInThread(heartBeatClosure, new Status(RaftError.EAGAIN,
                    "Fail to send heartbeat to peer %s, group %s", this.options.getPeerId(), this.options.getGroupId()));
            }
            return;
        }
        try {
            final long monotonicSendTimeMs = Utils.monotonicMs();

            if (isHeartbeat) {
                final AppendEntriesRequest request = rb.build();
                // Sending a heartbeat request
                this.heartbeatCounter++;
                RpcResponseClosure<AppendEntriesResponse> heartbeatDone;
                // Prefer passed-in closure.
                if (heartBeatClosure != null) {
                    heartbeatDone = heartBeatClosure;
                } else {
                    heartbeatDone = new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                        @Override
                        public void run(final Status status) {
                            onHeartbeatReturned(Replicator.this.id, status, request, getResponse(), monotonicSendTimeMs);
                        }
                    };
                }
                this.heartbeatInFly = this.rpcService.appendEntries(this.options.getPeerId().getEndpoint(), request,
                    this.options.getElectionTimeoutMs() / 2, heartbeatDone);
            } else {
                // No entries and has empty data means a probe request.
                // TODO(boyan) refactor, adds a new flag field?
                rb.setData(ByteString.EMPTY);
                final AppendEntriesRequest request = rb.build();
                // Sending a probe request.
                this.statInfo.runningState = RunningState.APPENDING_ENTRIES;
                this.statInfo.firstLogIndex = this.nextIndex;
                this.statInfo.lastLogIndex = this.nextIndex - 1;
                this.probeCounter++;
                setState(State.Probe);
                final int stateVersion = this.version;
                final int seq = getAndIncrementReqSeq();
                final Future<Message> rpcFuture = this.rpcService.appendEntries(this.options.getPeerId().getEndpoint(),
                    request, -1, new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                        @Override
                        public void run(final Status status) {
                            onRpcReturned(Replicator.this.id, RequestType.AppendEntries, status, request,
                                getResponse(), seq, stateVersion, monotonicSendTimeMs);
                        }

                    });

                addInflight(RequestType.AppendEntries, this.nextIndex, 0, 0, seq, rpcFuture);
            }
            LOG.debug("Node {} send HeartbeatRequest to {} term {} lastCommittedIndex {}", this.options.getNode()
                .getNodeId(), this.options.getPeerId(), this.options.getTerm(), rb.getCommittedIndex());
        } finally {
                unlockId();
        }
    }

    boolean prepareEntry(final long nextSendingIndex, final int offset, final RaftOutter.EntryMeta.Builder emb,
                         final RecyclableByteBufferList dateBuffer) {
        if (dateBuffer.getCapacity() >= this.raftOptions.getMaxBodySize()) {
            return false;
        }
        final long logIndex = nextSendingIndex + offset;
        final LogEntry entry = this.options.getLogManager().getEntry(logIndex);
        if (entry == null) {
            return false;
        }
        emb.setTerm(entry.getId().getTerm());
        if (entry.hasChecksum()) {
            emb.setChecksum(entry.getChecksum()); // since 1.2.6
        }
        emb.setType(entry.getType());
        if (entry.getPeers() != null) {
            Requires.requireTrue(!entry.getPeers().isEmpty(), "Empty peers at logIndex=%d", logIndex);
            fillMetaPeers(emb, entry);
        } else {
            Requires.requireTrue(entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION,
                "Empty peers but is ENTRY_TYPE_CONFIGURATION type at logIndex=%d", logIndex);
        }
        final int remaining = entry.getData() != null ? entry.getData().remaining() : 0;
        emb.setDataLen(remaining);
        if (entry.getData() != null) {
            // should slice entry data
            dateBuffer.add(entry.getData().slice());
        }
        return true;
    }

    private void fillMetaPeers(final RaftOutter.EntryMeta.Builder emb, final LogEntry entry) {
        for (final PeerId peer : entry.getPeers()) {
            emb.addPeers(peer.toString());
        }
        if (entry.getOldPeers() != null) {
            for (final PeerId peer : entry.getOldPeers()) {
                emb.addOldPeers(peer.toString());
            }
        }
        if (entry.getLearners() != null) {
            for (final PeerId peer : entry.getLearners()) {
                emb.addLearners(peer.toString());
            }
        }
        if (entry.getOldLearners() != null) {
            for (final PeerId peer : entry.getOldLearners()) {
                emb.addOldLearners(peer.toString());
            }
        }
    }

    public static ThreadId start(final ReplicatorOptions opts, final RaftOptions raftOptions) {
        if (opts.getLogManager() == null || opts.getBallotBox() == null || opts.getNode() == null) {
            throw new IllegalArgumentException("Invalid ReplicatorOptions.");
        }
        final Replicator r = new Replicator(opts, raftOptions);
        if (!r.rpcService.connect(opts.getPeerId().getEndpoint())) {
            LOG.error("Fail to init sending channel to {}, group: {}.", opts.getPeerId(), opts.getGroupId());
            // Return and it will be retried later.
            return null;
        }

        // Register replicator metric set.
        final MetricRegistry metricRegistry = opts.getNode().getNodeMetrics().getMetricRegistry();
        if (metricRegistry != null) {
            try {
                if (!metricRegistry.getNames().contains(r.metricName)) {
                    metricRegistry.register(r.metricName, new ReplicatorMetricSet(opts, r));
                }
            } catch (final IllegalArgumentException e) {
                // ignore
            }
        }

        // Start replication
        r.id = new ThreadId(r, r);
        r.id.lock();
        notifyReplicatorStatusListener(r, ReplicatorEvent.CREATED);
        LOG.info("Replicator [group: {}, peer: {}, type: {}] is started", r.options.getGroupId(), r.options.getPeerId(), r.options.getReplicatorType());
        r.catchUpClosure = null;
        r.lastRpcSendTimestamp = Utils.monotonicMs();
        r.startHeartbeatTimer(Utils.nowMs());
        // id.unlock in sendEmptyEntries
        r.sendProbeRequest();
        return r.id;
    }

    private String getReplicatorMetricName(final ReplicatorOptions opts) {
        return "replicator-" + opts.getNode().getGroupId() + "/" + opts.getPeerId();
    }

    public static void waitForCaughtUp(final String groupId, final ThreadId id, final long maxMargin, final long dueTime,
                                       final CatchUpClosure done) {
        final Replicator r = (Replicator) id.lock();

        if (r == null) {
            RpcUtils.runClosureInThread(done, new Status(RaftError.EINVAL, "No such replicator"));
            return;
        }
        try {
            if (r.catchUpClosure != null) {
                LOG.error("Previous wait_for_caught_up is not over, peer: {}, group id: {}", r.options.getPeerId(), r.options.getGroupId());
                ThreadPoolsFactory.runClosureInThread(groupId, done, new Status(RaftError.EINVAL, "Duplicated call"));
                return;
            }
            done.setMaxMargin(maxMargin);
            if (dueTime > 0) {
                done.setTimer(r.timerManager.schedule(() -> onCatchUpTimedOut(id), dueTime - Utils.nowMs(),
                    TimeUnit.MILLISECONDS));
            }
            r.catchUpClosure = done;
        } finally {
            id.unlock();
        }
    }

    @Override
    public String toString() {
        return "Replicator [state=" + getState() + ", statInfo=" + this.statInfo + ", peerId="
                + this.options.getPeerId() + ", groupId=" + this.options.getGroupId() + ", waitId=" + this.waitId
                + ", type=" + this.options.getReplicatorType() + "]";
    }

    static void onBlockTimeoutInNewThread(final ThreadId id) {
        if (id != null) {
            continueSending(id, RaftError.ETIMEDOUT.getNumber());
        }
    }

    /**
     * Unblock and continue sending right now.
     */
    static void unBlockAndSendNow(final ThreadId id) {
        if (id == null) {
            // It was destroyed already
            return;
        }
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        try {
            if (r.blockTimer != null) {
                if (r.blockTimer.cancel(true)) {
                    onBlockTimeout(id);
                }
            }
        } finally {
            id.unlock();
        }
    }

    static boolean continueSending(final ThreadId id, final int errCode) {
        if (id == null) {
            //It was destroyed already
            return true;
        }
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        
        if (errCode == RaftError.ETIMEDOUT.getNumber()) {
            r.blockTimer = null;
            // Send empty entries after block timeout to check the correct
            // _next_index otherwise the replicator is likely waits in            executor.shutdown();
            // _wait_more_entries and no further logs would be replicated even if the
            // last_index of this followers is less than |next_index - 1|
            r.sendProbeRequest();
        } else if (errCode != RaftError.ESTOP.getNumber()) {
        	// Only reset waitId before sending entries, fixed #842, #838
        	r.waitId = -1;
            // id is unlock in _send_entries
            r.sendEntries();
        } else {
            LOG.warn("Replicator {} stops sending entries.", id);
            id.unlock();
        }
        return true;
    }

    static void onBlockTimeout(final ThreadId arg) {
        RpcUtils.runInThread(() -> onBlockTimeoutInNewThread(arg));
    }

    void block(final long startTimeMs, @SuppressWarnings("unused") final int errorCode) {
        // TODO: Currently we don't care about error_code which indicates why the
        // very RPC fails. To make it better there should be different timeout for
        // each individual error (e.g. we don't need check every
        // heartbeat_timeout_ms whether a dead follower has come back), but it's just
        // fine now.
        if(this.blockTimer != null) {
            // already in blocking state,return immediately.
            unlockId();
            return;
        }
        this.blockCounter++;
        final long dueTime = startTimeMs + this.options.getDynamicHeartBeatTimeoutMs();
        try {
            LOG.debug("Blocking {} for {} ms", this.options.getPeerId(), this.options.getDynamicHeartBeatTimeoutMs());
            this.blockTimer = this.timerManager.schedule(() -> onBlockTimeout(this.id), dueTime - Utils.nowMs(),
                TimeUnit.MILLISECONDS);
            this.statInfo.runningState = RunningState.BLOCKING;
            unlockId();
        } catch (final Exception e) {
            this.blockTimer = null;
            LOG.error("Fail to add timer for replicator {}", this, e);
            // id unlock in sendEmptyEntries.
            sendProbeRequest();
        }
    }

    @Override
    public void onError(final ThreadId id, final Object data, final int errorCode) {
        final Replicator r = (Replicator) data;
        if (errorCode == RaftError.ESTOP.getNumber()) {
            try {
                for (final Inflight inflight : r.inflights) {
                    if (inflight != r.rpcInFly) {
                        inflight.rpcFuture.cancel(true);
                    }
                }
                if (r.rpcInFly != null) {
                    r.rpcInFly.rpcFuture.cancel(true);
                    r.rpcInFly = null;
                }
                if (r.heartbeatInFly != null) {
                    r.heartbeatInFly.cancel(true);
                    r.heartbeatInFly = null;
                }
                if (r.timeoutNowInFly != null) {
                    r.timeoutNowInFly.cancel(true);
                    r.timeoutNowInFly = null;
                }
                if (r.heartbeatTimer != null) {
                    r.heartbeatTimer.cancel(true);
                    r.heartbeatTimer = null;
                }
                if (r.blockTimer != null) {
                    r.blockTimer.cancel(true);
                    r.blockTimer = null;
                }
                if (r.waitId >= 0) {
                    r.options.getLogManager().removeWaiter(r.waitId);
                }
                r.notifyOnCaughtUp(errorCode, true);
            } finally {
                r.destroy();
            }
        } else if (errorCode == RaftError.ETIMEDOUT.getNumber()) {
            RpcUtils.runInThread(() -> sendHeartbeat(id));
        } else {
            // noinspection ConstantConditions
            Requires.requireTrue(false, "Unknown error code " + errorCode + " for replicator: " + r);
        }
    }

    private static void onCatchUpTimedOut(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        try {
            r.notifyOnCaughtUp(RaftError.ETIMEDOUT.getNumber(), false);
        } finally {
            id.unlock();
        }
    }

    private void notifyOnCaughtUp(final int code, final boolean beforeDestroy) {
        if (this.catchUpClosure == null) {
            return;
        }
        if (code != RaftError.ETIMEDOUT.getNumber()) {
            if (this.nextIndex - 1 + this.catchUpClosure.getMaxMargin() < this.options.getLogManager()
                .getLastLogIndex()) {
                return;
            }
            if (this.catchUpClosure.isErrorWasSet()) {
                return;
            }
            this.catchUpClosure.setErrorWasSet(true);
            if (code != RaftError.SUCCESS.getNumber()) {
                this.catchUpClosure.getStatus().setError(code, RaftError.describeCode(code));
            }
            if (this.catchUpClosure.hasTimer()) {
                if (!beforeDestroy && !this.catchUpClosure.getTimer().cancel(true)) {
                    // There's running timer task, let timer task trigger
                    // on_caught_up to void ABA problem
                    return;
                }
            }
        } else {
            // timed out
            if (!this.catchUpClosure.isErrorWasSet()) {
                this.catchUpClosure.getStatus().setError(code, RaftError.describeCode(code));
            }
        }
        final CatchUpClosure savedClosure = this.catchUpClosure;
        this.catchUpClosure = null;
        RpcUtils.runClosureInThread(savedClosure, savedClosure.getStatus());
    }

    private void onTimeout(final ThreadId id) {
        if (id != null) {
            id.setError(RaftError.ETIMEDOUT.getNumber());
        } else {
            LOG.warn("Replicator {} id is null when timeout, maybe it's destroyed.", this);
        }
    }

    void destroy() {
        final ThreadId savedId = this.id;
        LOG.info("Replicator {} is going to quit", savedId);
        releaseReader();
        // Unregister replicator metric set
        if (this.nodeMetrics.isEnabled()) {
            this.nodeMetrics.getMetricRegistry() //
                .removeMatching(MetricFilter.startsWith(this.metricName));
        }
        setState(State.Destroyed);
        notifyReplicatorStatusListener((Replicator) savedId.getData(), ReplicatorEvent.DESTROYED);
        savedId.unlockAndDestroy();
        this.id = null;
    }

    private void releaseReader() {
        if (this.reader != null) {
            Utils.closeQuietly(this.reader);
            this.reader = null;
        }
    }

    static void onHeartbeatReturned(final ThreadId id, final Status status, final AppendEntriesRequest request,
                                    final AppendEntriesResponse response, final long rpcSendTime) {
        if (id == null) {
            // replicator already was destroyed.
            return;
        }
        final long startTimeMs = Utils.nowMs();
        Replicator r;
        if ((r = (Replicator) id.lock()) == null) {
            return;
        }
        boolean doUnlock = true;
        try {
            final boolean isLogDebugEnabled = LOG.isDebugEnabled();
            StringBuilder sb = null;
            if (isLogDebugEnabled) {
                sb = new StringBuilder("Node ") //
                    .append(r.options.getGroupId()) //
                    .append(':') //
                    .append(r.options.getServerId()) //
                    .append(" received HeartbeatResponse from ") //
                    .append(r.options.getPeerId()) //
                    .append(" prevLogIndex=") //
                    .append(request.getPrevLogIndex()) //
                    .append(" prevLogTerm=") //
                    .append(request.getPrevLogTerm());
            }
            if (!status.isOk()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, sleep, status=") //
                        .append(status);
                    LOG.debug(sb.toString());
                }
                r.setState(State.Probe);
                notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
                if ((r.consecutiveErrorTimes++) % 10 == 0) {
                    LOG.warn("Fail to issue RPC to {}, consecutiveErrorTimes={}, error={}, groupId={}", r.options.getPeerId(),
                        r.consecutiveErrorTimes, status, r.options.getGroupId());
                }
                r.startHeartbeatTimer(startTimeMs);
                return;
            }
            r.consecutiveErrorTimes = 0;
            if (response.getTerm() > r.options.getTerm()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, greater term ") //
                        .append(response.getTerm()) //
                        .append(" expect term ") //
                        .append(r.options.getTerm());
                    LOG.debug(sb.toString());
                }
                final NodeImpl node = r.options.getNode();
                r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
                r.destroy();
                node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Leader receives higher term heartbeat_response from peer:%s, group:%s", r.options.getPeerId(), r.options.getGroupId()));
                return;
            }
            if (!response.getSuccess() && response.hasLastLogIndex()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, response term ") //
                        .append(response.getTerm()) //
                        .append(" lastLogIndex ") //
                        .append(response.getLastLogIndex());
                    LOG.debug(sb.toString());
                }
                LOG.warn("Heartbeat to peer {} failure, try to send a probe request, groupId={}.", r.options.getPeerId(), r.options.getGroupId());
                doUnlock = false;
                r.sendProbeRequest();
                r.startHeartbeatTimer(startTimeMs);
                return;
            }
            if (isLogDebugEnabled) {
                LOG.debug(sb.toString());
            }
            if (rpcSendTime > r.lastRpcSendTimestamp) {
                r.lastRpcSendTimestamp = rpcSendTime;
            }
            r.startHeartbeatTimer(startTimeMs);
        } finally {
            if (doUnlock) {
                id.unlock();
            }
        }
    }

    @SuppressWarnings("ContinueOrBreakFromFinallyBlock")
    static void onRpcReturned(final ThreadId id, final RequestType reqType, final Status status, final Message request,
                              final Message response, final int seq, final int stateVersion, final long rpcSendTime) {
        if (id == null) {
            return;
        }
        final long startTimeMs = Utils.nowMs();
        Replicator r;
        if ((r = (Replicator) id.lock()) == null) {
            return;
        }

        if (stateVersion != r.version) {
            LOG.debug(
                "Replicator {} ignored old version response {}, current version is {}, request is {}\n, and response is {}\n, status is {}.",
                r, stateVersion, r.version, request, response, status);
            id.unlock();
            return;
        }

        final PriorityQueue<RpcResponse> holdingQueue = r.pendingResponses;
        holdingQueue.add(new RpcResponse(reqType, seq, status, request, response, rpcSendTime));

        if (holdingQueue.size() > r.raftOptions.getMaxReplicatorInflightMsgs()) {
            LOG.warn("Too many pending responses {} for replicator {}, maxReplicatorInflightMsgs={}",
                holdingQueue.size(), r, r.raftOptions.getMaxReplicatorInflightMsgs());
            r.resetInflights();
            r.setState(State.Probe);
            r.sendProbeRequest();
            return;
        }

        boolean continueSendEntries = false;

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Replicator ") //
                .append(r) //
                .append(" is processing RPC responses, ");
        }
        try {
            int processed = 0;
            while (!holdingQueue.isEmpty()) {
                final RpcResponse queuedPipelinedResponse = holdingQueue.peek();

                // Sequence mismatch, waiting for next response.
                if (queuedPipelinedResponse.seq != r.requiredNextSeq) {
                    if (processed > 0) {
                        if (isLogDebugEnabled) {
                            sb.append("has processed ") //
                                .append(processed) //
                                .append(" responses, ");
                        }
                        break;
                    } else {
                        // Do not processed any responses, UNLOCK id and return.
                        continueSendEntries = false;
                        id.unlock();
                        return;
                    }
                }
                holdingQueue.remove();
                processed++;
                final Inflight inflight = r.pollInflight();
                if (inflight == null) {
                    // The previous in-flight requests were cleared.
                    if (isLogDebugEnabled) {
                        sb.append("ignore response because request not found: ") //
                            .append(queuedPipelinedResponse) //
                            .append(",\n");
                    }
                    continue;
                }
                if (inflight.seq != queuedPipelinedResponse.seq) {
                    // reset state
                    LOG.warn(
                        "Replicator {} response sequence out of order, expect {}, but it is {}, reset state to try again.",
                        r, inflight.seq, queuedPipelinedResponse.seq);
                    r.resetInflights();
                    r.setState(State.Probe);
                    continueSendEntries = false;
                    r.block(Utils.nowMs(), RaftError.EREQUEST.getNumber());
                    return;
                }
                try {
                    switch (queuedPipelinedResponse.requestType) {
                        case AppendEntries:
                            continueSendEntries = onAppendEntriesReturned(id, inflight, queuedPipelinedResponse.status,
                                (AppendEntriesRequest) queuedPipelinedResponse.request,
                                (AppendEntriesResponse) queuedPipelinedResponse.response, rpcSendTime, startTimeMs, r);
                            break;
                        case Snapshot:
                            continueSendEntries = onInstallSnapshotReturned(id, r, queuedPipelinedResponse.status,
                                (InstallSnapshotRequest) queuedPipelinedResponse.request,
                                (InstallSnapshotResponse) queuedPipelinedResponse.response);
                            break;
                    }
                } finally {
                    if (continueSendEntries) {
                        // Success, increase the response sequence.
                        r.getAndIncrementRequiredNextSeq();
                    } else {
                        // The id is already unlocked in onAppendEntriesReturned/onInstallSnapshotReturned, we SHOULD break out.
                        break;
                    }
                }
            }
        } finally {
            if (isLogDebugEnabled) {
                sb.append("after processed, continue to send entries: ") //
                    .append(continueSendEntries);
                LOG.debug(sb.toString());
            }
            if (continueSendEntries) {
                // unlock in sendEntries.
                r.sendEntries();
            }
        }
    }

    /**
     * Reset in-flight state.
     */
    void resetInflights() {
        this.version++;
        this.inflights.clear();
        this.pendingResponses.clear();
        final int rs = Math.max(this.reqSeq, this.requiredNextSeq);
        this.reqSeq = this.requiredNextSeq = rs;
        releaseReader();
    }

    private static boolean onAppendEntriesReturned(final ThreadId id, final Inflight inflight, final Status status,
                                                   final AppendEntriesRequest request,
                                                   final AppendEntriesResponse response, final long rpcSendTime,
                                                   final long startTimeMs, final Replicator r) {
        if (inflight.startIndex != request.getPrevLogIndex() + 1) {
            LOG.warn(
                "Replicator {} received invalid AppendEntriesResponse, in-flight startIndex={}, request prevLogIndex={}, reset the replicator state and probe again.",
                r, inflight.startIndex, request.getPrevLogIndex());
            r.resetInflights();
            r.setState(State.Probe);
            // unlock id in sendEmptyEntries
            r.sendProbeRequest();
            return false;
        }
        // record metrics
        if (request.getEntriesCount() > 0) {
            r.nodeMetrics.recordLatency("replicate-entries", Utils.monotonicMs() - rpcSendTime);
            r.nodeMetrics.recordSize("replicate-entries-count", request.getEntriesCount());
            r.nodeMetrics.recordSize("replicate-entries-bytes", request.getData() != null ? request.getData().size()
                : 0);
        }

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Node ") //
                .append(r.options.getGroupId()) //
                .append(':') //
                .append(r.options.getServerId()) //
                .append(" received AppendEntriesResponse from ") //
                .append(r.options.getPeerId()) //
                .append(" prevLogIndex=") //
                .append(request.getPrevLogIndex()) //
                .append(" prevLogTerm=") //
                .append(request.getPrevLogTerm()) //
                .append(" count=") //
                .append(request.getEntriesCount());
        }
        if (!status.isOk()) {
            // If the follower crashes, any RPC to the follower fails immediately,
            // so we need to block the follower for a while instead of looping until
            // it comes back or be removed
            // dummy_id is unlock in block
            if (isLogDebugEnabled) {
                sb.append(" fail, sleep, status=") //
                    .append(status);
                LOG.debug(sb.toString());
            }
            notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
            if ((r.consecutiveErrorTimes++) % 10 == 0) {
                LOG.warn("Fail to issue RPC to {}, consecutiveErrorTimes={}, error={}, groupId={}", r.options.getPeerId(),
                    r.consecutiveErrorTimes, status, r.options.getGroupId());
            }
            r.resetInflights();
            r.setState(State.Probe);
            // unlock in in block
            r.block(startTimeMs, status.getCode());
            return false;
        }
        r.consecutiveErrorTimes = 0;
        if (!response.getSuccess()) {
             // Target node is is busy, sleep for a while.
            if(response.getErrorResponse().getErrorCode() == RaftError.EBUSY.getNumber()) {
              if (isLogDebugEnabled) {
                sb.append(" is busy, sleep, errorMsg='") //
                    .append(response.getErrorResponse().getErrorMsg()).append("'");
                LOG.debug(sb.toString());
              }
              r.resetInflights();
              r.setState(State.Probe);
              // unlock in in block
              r.block(startTimeMs, status.getCode());
              return false;
            }

            if (response.getTerm() > r.options.getTerm()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, greater term ") //
                        .append(response.getTerm()) //
                        .append(" expect term ") //
                        .append(r.options.getTerm());
                    LOG.debug(sb.toString());
                }
                final NodeImpl node = r.options.getNode();
                r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
                r.destroy();
                node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Leader receives higher term heartbeat_response from peer:%s, group:%s", r.options.getPeerId(), r.options.getGroupId()));
                return false;
            }
            if (isLogDebugEnabled) {
                sb.append(" fail, find nextIndex remote lastLogIndex ").append(response.getLastLogIndex())
                    .append(" local nextIndex ").append(r.nextIndex);
                LOG.debug(sb.toString());
            }
            if (rpcSendTime > r.lastRpcSendTimestamp) {
                r.lastRpcSendTimestamp = rpcSendTime;
            }
            // Fail, reset the state to try again from nextIndex.
            r.resetInflights();
            // prev_log_index and prev_log_term doesn't match
            if (response.getLastLogIndex() + 1 < r.nextIndex) {
                LOG.debug("LastLogIndex at peer={} is {}", r.options.getPeerId(), response.getLastLogIndex());
                // The peer contains less logs than leader
                r.nextIndex = response.getLastLogIndex() + 1;
            } else {
                // The peer contains logs from old term which should be truncated,
                // decrease _last_log_at_peer by one to test the right index to keep
                if (r.nextIndex > 1) {
                    LOG.debug("logIndex={} dismatch", r.nextIndex);
                    r.nextIndex--;
                } else {
                    LOG.error("Peer={} declares that log at index=0 doesn't match, which is not supposed to happen, groupId={}",
                        r.options.getPeerId(), r.options.getGroupId());
                }
            }
            // dummy_id is unlock in _send_heartbeat
            r.sendProbeRequest();
            return false;
        }
        if (isLogDebugEnabled) {
            sb.append(", success");
            LOG.debug(sb.toString());
        }
        // success
        if (response.getTerm() != r.options.getTerm()) {
            r.resetInflights();
            r.setState(State.Probe);
            LOG.error("Fail, response term {} dismatch, expect term {}, peer {}, group {}", response.getTerm(),
                    r.options.getTerm(), r.options.getPeerId(), r.options.getGroupId());
            id.unlock();
            return false;
        }
        if (rpcSendTime > r.lastRpcSendTimestamp) {
            r.lastRpcSendTimestamp = rpcSendTime;
        }
        final int entriesSize = request.getEntriesCount();
        if (entriesSize > 0) {
            if (r.options.getReplicatorType().isFollower()) {
                // Only commit index when the response is from follower.
                r.options.getBallotBox().commitAt(r.nextIndex, r.nextIndex + entriesSize - 1, r.options.getPeerId());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Replicated logs in [{}, {}] to peer {}", r.nextIndex, r.nextIndex + entriesSize - 1,
                    r.options.getPeerId());
            }
        }

        r.setState(State.Replicate);
        r.blockTimer = null;
        r.nextIndex += entriesSize;
        r.hasSucceeded = true;
        r.notifyOnCaughtUp(RaftError.SUCCESS.getNumber(), false);
        // dummy_id is unlock in _send_entries
        if (r.timeoutNowIndex > 0 && r.timeoutNowIndex < r.nextIndex) {
            r.sendTimeoutNow(false, false);
        }
        return true;
    }

    private boolean fillCommonFields(final AppendEntriesRequest.Builder rb, long prevLogIndex, final boolean isHeartbeat) {
        final long prevLogTerm = this.options.getLogManager().getTerm(prevLogIndex);
        if (prevLogTerm == 0 && prevLogIndex != 0) {
            if (!isHeartbeat) {
                Requires.requireTrue(prevLogIndex < this.options.getLogManager().getFirstLogIndex());
                LOG.debug("logIndex={} was compacted", prevLogIndex);
                return false;
            } else {
                // The log at prev_log_index has been compacted, which indicates
                // we is or is going to install snapshot to the follower. So we let
                // both prev_log_index and prev_log_term be 0 in the heartbeat
                // request so that follower would do nothing besides updating its
                // leader timestamp.
                prevLogIndex = 0;
            }
        }
        rb.setTerm(this.options.getTerm());
        rb.setGroupId(this.options.getGroupId());
        rb.setServerId(this.options.getServerId().toString());
        rb.setPeerId(this.options.getPeerId().toString());
        rb.setPrevLogIndex(prevLogIndex);
        rb.setPrevLogTerm(prevLogTerm);
        rb.setCommittedIndex(this.options.getBallotBox().getLastCommittedIndex());
        return true;
    }

    private void waitMoreEntries(final long nextWaitIndex) {
        try {
            LOG.debug("Node {} waits more entries", this.options.getNode().getNodeId());
            if (this.waitId >= 0) {
                return;
            }
            this.waitId = this.options.getLogManager().wait(nextWaitIndex - 1,
                (arg, errorCode) -> continueSending((ThreadId) arg, errorCode), this.id);
            this.statInfo.runningState = RunningState.IDLE;
        } finally {
            unlockId();
        }
    }

    /**
     * Send as many requests as possible.
     */
    void sendEntries() {
        boolean doUnlock = true;
        try {
            long prevSendIndex = -1;
            while (true) {
                final long nextSendingIndex = getNextSendIndex();
                if (nextSendingIndex > prevSendIndex) {
                    if (sendEntries(nextSendingIndex)) {
                        prevSendIndex = nextSendingIndex;
                    } else {
                        doUnlock = false;
                        // id already unlock in sendEntries when it returns false.
                        break;
                    }
                } else {
                    break;
                }
            }
        } finally {
            if (doUnlock) {
                unlockId();
            }
        }

    }

    /**
     * Send log entries to follower, returns true when success, otherwise false and unlock the id.
     *
     * @param nextSendingIndex next sending index
     * @return send result.
     */
    private boolean sendEntries(final long nextSendingIndex) {
        final AppendEntriesRequest.Builder rb = AppendEntriesRequest.newBuilder();
        if (!fillCommonFields(rb, nextSendingIndex - 1, false)) {
            // unlock id in installSnapshot
            installSnapshot();
            return false;
        }

        ByteBufferCollector dataBuf = null;
        final int maxEntriesSize = this.raftOptions.getMaxEntriesSize();
        final RecyclableByteBufferList byteBufList = RecyclableByteBufferList.newInstance();
        try {
            for (int i = 0; i < maxEntriesSize; i++) {
                final RaftOutter.EntryMeta.Builder emb = RaftOutter.EntryMeta.newBuilder();
                if (!prepareEntry(nextSendingIndex, i, emb, byteBufList)) {
                    break;
                }
                rb.addEntries(emb.build());
            }
            if (rb.getEntriesCount() == 0) {
                if (nextSendingIndex < this.options.getLogManager().getFirstLogIndex()) {
                    installSnapshot();
                    return false;
                }
                // _id is unlock in _wait_more
                waitMoreEntries(nextSendingIndex);
                return false;
            }
            if (byteBufList.getCapacity() > 0) {
                dataBuf = ByteBufferCollector.allocateByRecyclers(byteBufList.getCapacity());
                for (final ByteBuffer b : byteBufList) {
                    dataBuf.put(b);
                }
                final ByteBuffer buf = dataBuf.getBuffer();
                BufferUtils.flip(buf);
                rb.setData(ZeroByteStringHelper.wrap(buf));
            }
        } finally {
            RecycleUtil.recycle(byteBufList);
        }

        final AppendEntriesRequest request = rb.build();
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Node {} send AppendEntriesRequest to {} term {} lastCommittedIndex {} prevLogIndex {} prevLogTerm {} logIndex {} count {}",
                this.options.getNode().getNodeId(), this.options.getPeerId(), this.options.getTerm(),
                request.getCommittedIndex(), request.getPrevLogIndex(), request.getPrevLogTerm(), nextSendingIndex,
                request.getEntriesCount());
        }
        this.statInfo.runningState = RunningState.APPENDING_ENTRIES;
        this.statInfo.firstLogIndex = rb.getPrevLogIndex() + 1;
        this.statInfo.lastLogIndex = rb.getPrevLogIndex() + rb.getEntriesCount();

        final Recyclable recyclable = dataBuf;
        final int v = this.version;
        final long monotonicSendTimeMs = Utils.monotonicMs();
        final int seq = getAndIncrementReqSeq();

        this.appendEntriesCounter++;
        Future<Message> rpcFuture = null;
        try {
            rpcFuture = this.rpcService.appendEntries(this.options.getPeerId().getEndpoint(), request, -1,
                new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                    @Override
                    public void run(final Status status) {
                        RecycleUtil.recycle(recyclable); // TODO: recycle on send success, not response received.
                        onRpcReturned(Replicator.this.id, RequestType.AppendEntries, status, request, getResponse(),
                            seq, v, monotonicSendTimeMs);
                    }
                });
        } catch (final Throwable t) {
            RecycleUtil.recycle(recyclable);
            ThrowUtil.throwException(t);
        }
        addInflight(RequestType.AppendEntries, nextSendingIndex, request.getEntriesCount(), request.getData().size(),
            seq, rpcFuture);

        return true;
    }

    public static void sendHeartbeat(final ThreadId id, final RpcResponseClosure<AppendEntriesResponse> closure) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            RpcUtils.runClosureInThread(closure, new Status(RaftError.EHOSTDOWN, "Peer %s is not connected", id));
            return;
        }
        //id unlock in send empty entries.
        r.sendEmptyEntries(true, closure);
    }

    private static void sendHeartbeat(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        // unlock in sendEmptyEntries
        r.sendEmptyEntries(true);
    }

    private void sendProbeRequest() {
        sendEmptyEntries(false);
    }

    @SuppressWarnings("SameParameterValue")
    private void sendTimeoutNow(final boolean unlockId, final boolean stopAfterFinish) {
        sendTimeoutNow(unlockId, stopAfterFinish, -1);
    }

    private void sendTimeoutNow(final boolean unlockId, final boolean stopAfterFinish, final int timeoutMs) {
        final TimeoutNowRequest.Builder rb = TimeoutNowRequest.newBuilder();
        rb.setTerm(this.options.getTerm());
        rb.setGroupId(this.options.getGroupId());
        rb.setServerId(this.options.getServerId().toString());
        rb.setPeerId(this.options.getPeerId().toString());
        try {
            if (!stopAfterFinish) {
                // This RPC is issued by transfer_leadership, save this call_id so that
                // the RPC can be cancelled by stop.
                this.timeoutNowInFly = timeoutNow(rb, false, timeoutMs);
                this.timeoutNowIndex = 0;
            } else {
                timeoutNow(rb, true, timeoutMs);
            }
        } finally {
            if (unlockId) {
                unlockId();
            }
        }

    }

    private Future<Message> timeoutNow(final TimeoutNowRequest.Builder rb, final boolean stopAfterFinish,
                                       final int timeoutMs) {
        final TimeoutNowRequest request = rb.build();
        return this.rpcService.timeoutNow(this.options.getPeerId().getEndpoint(), request, timeoutMs,
            new RpcResponseClosureAdapter<TimeoutNowResponse>() {

                @Override
                public void run(final Status status) {
                    if (Replicator.this.id != null) {
                        onTimeoutNowReturned(Replicator.this.id, status, request, getResponse(), stopAfterFinish);
                    }
                }

            });
    }

    @SuppressWarnings("unused")
    static void onTimeoutNowReturned(final ThreadId id, final Status status, final TimeoutNowRequest request,
                                     final TimeoutNowResponse response, final boolean stopAfterFinish) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Node "). //
                append(r.options.getGroupId()).append(":").append(r.options.getServerId()). //
                append(" received TimeoutNowResponse from "). //
                append(r.options.getPeerId());
        }
        if (!status.isOk()) {
            if (isLogDebugEnabled) {
                sb.append(" fail:").append(status);
                LOG.debug(sb.toString());
            }
            notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
            if (stopAfterFinish) {
                r.notifyOnCaughtUp(RaftError.ESTOP.getNumber(), true);
                r.destroy();
            } else {
                id.unlock();
            }
            return;
        }
        if (isLogDebugEnabled) {
            sb.append(response.getSuccess() ? " success" : " fail");
            LOG.debug(sb.toString());
        }
        if (response.getTerm() > r.options.getTerm()) {
            final NodeImpl node = r.options.getNode();
            r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
            r.destroy();
            node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                "Leader receives higher term timeout_now_response from peer:%s, group:%s", r.options.getPeerId(), r.options.getGroupId()));
            return;
        }
        if (stopAfterFinish) {
            r.notifyOnCaughtUp(RaftError.ESTOP.getNumber(), true);
            r.destroy();
        } else {
            id.unlock();
        }

    }

    public static boolean stop(final ThreadId id) {
        id.setError(RaftError.ESTOP.getNumber());
        return true;
    }

    public static boolean join(final ThreadId id) {
        id.join();
        return true;
    }

    public static long getLastRpcSendTimestamp(final ThreadId id) {
        final Replicator r = (Replicator) id.getData();
        if (r == null) {
            return 0L;
        }
        return r.lastRpcSendTimestamp;
    }

    public static boolean transferLeadership(final ThreadId id, final long logIndex) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        // dummy is unlock in _transfer_leadership
        return r.transferLeadership(logIndex);
    }

    private boolean transferLeadership(final long logIndex) {
        if (this.hasSucceeded && this.nextIndex > logIndex) {
            // _id is unlock in _send_timeout_now
            sendTimeoutNow(true, false);
            return true;
        }
        // Register log_index so that _on_rpc_return trigger
        // _send_timeout_now if _next_index reaches log_index
        this.timeoutNowIndex = logIndex;
        unlockId();
        return true;
    }

    public static boolean stopTransferLeadership(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        r.timeoutNowIndex = 0;
        id.unlock();
        return true;
    }

    public static boolean sendTimeoutNowAndStop(final ThreadId id, final int timeoutMs) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        // id unlock in sendTimeoutNow
        r.sendTimeoutNow(true, true, timeoutMs);
        return true;
    }

    public static long getNextIndex(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return 0;
        }
        long nextIdx = 0;
        if (r.hasSucceeded) {
            nextIdx = r.nextIndex;
        }
        id.unlock();
        return nextIdx;
    }

    private void unlockId() {
        if (this.id == null) {
            return;
        }
        this.id.unlock();
    }

}
