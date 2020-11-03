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
package com.alipay.sofa.jraft.rpc.impl.core;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequestHeader;
import com.alipay.sofa.jraft.rpc.impl.ConnectionClosedEventListener;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.ConcurrentHashSet;
import com.alipay.sofa.jraft.util.concurrent.MpscSingleThreadExecutor;
import com.alipay.sofa.jraft.util.concurrent.SingleThreadExecutor;
import com.google.protobuf.Message;

/**
 * Append entries request processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 *         2018-Apr-04 3:00:13 PM
 */
public class AppendEntriesRequestProcessor extends NodeRequestProcessor<AppendEntriesRequest> implements
                                                                                             ConnectionClosedEventListener {

    static final String PAIR_ATTR = "jraft-peer-pairs";

    /**
     * Peer executor selector.
     *
     * @author dennis
     */
    final class PeerExecutorSelector implements RpcProcessor.ExecutorSelector {

        PeerExecutorSelector() {
            super();
        }

        @Override
        public Executor select(final String reqClass, final Object reqHeader) {
            final AppendEntriesRequestHeader header = (AppendEntriesRequestHeader) reqHeader;
            final String groupId = header.getGroupId();
            final String peerId = header.getPeerId();
            final String serverId = header.getServerId();

            final PeerId peer = new PeerId();

            if (!peer.parse(peerId)) {
                return executor();
            }

            final Node node = NodeManager.getInstance().get(groupId, peer);

            if (node == null || !node.getRaftOptions().isReplicatorPipeline()) {
                return executor();
            }

            // The node enable pipeline, we should ensure bolt support it.
            RpcFactoryHelper.rpcFactory().ensurePipeline();

            final PeerRequestContext ctx = getOrCreatePeerRequestContext(groupId, pairOf(peerId, serverId), null);

            return ctx.executor;
        }
    }

    /**
     * RpcRequestClosure that will send responses in pipeline mode.
     *
     * @author dennis
     */
    class SequenceRpcRequestClosure extends RpcRequestClosure {

        private final int      reqSequence;
        private final String   groupId;
        private final PeerPair pair;
        private final boolean  isHeartbeat;

        public SequenceRpcRequestClosure(final RpcRequestClosure parent, final Message defaultResp,
                                         final String groupId, final PeerPair pair, final int sequence,
                                         final boolean isHeartbeat) {
            super(parent.getRpcCtx(), defaultResp);
            this.reqSequence = sequence;
            this.groupId = groupId;
            this.pair = pair;
            this.isHeartbeat = isHeartbeat;
        }

        @Override
        public void sendResponse(final Message msg) {
            if (this.isHeartbeat) {
                super.sendResponse(msg);
            } else {
                sendSequenceResponse(this.groupId, this.pair, this.reqSequence, getRpcCtx(), msg);
            }
        }
    }

    /**
     * Response message wrapper with a request sequence number and asyncContext.done
     *
     * @author dennis
     */
    static class SequenceMessage implements Comparable<SequenceMessage> {
        public final Message     msg;
        private final int        sequence;
        private final RpcContext rpcCtx;

        public SequenceMessage(final RpcContext rpcCtx, final Message msg, final int sequence) {
            super();
            this.rpcCtx = rpcCtx;
            this.msg = msg;
            this.sequence = sequence;
        }

        /**
         * Send the response.
         */
        void sendResponse() {
            this.rpcCtx.sendResponse(this.msg);
        }

        /**
         * Order by sequence number
         */
        @Override
        public int compareTo(final SequenceMessage o) {
            return Integer.compare(this.sequence, o.sequence);
        }
    }

    // constant pool for peer pair
    private final Map<String, Map<String, PeerPair>> pairConstants = new HashMap<>();

    PeerPair pairOf(final String peerId, final String serverId) {
        synchronized (this.pairConstants) {
            Map<String, PeerPair> pairs = this.pairConstants.computeIfAbsent(peerId, k -> new HashMap<>());

            PeerPair pair = pairs.computeIfAbsent(serverId, k -> new PeerPair(peerId, serverId));
            return pair;
        }
    }

    /**
     * A peer pair
     * @author boyan(boyan@antfin.com)
     *
     */
    static class PeerPair {
        // peer in local node
        final String local;
        // peer in remote node
        final String remote;

        PeerPair(final String local, final String remote) {
            super();
            this.local = local;
            this.remote = remote;
        }

        @Override
        public String toString() {
            return "PeerPair[" + this.local + " -> " + this.remote + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.local == null) ? 0 : this.local.hashCode());
            result = prime * result + ((this.remote == null) ? 0 : this.remote.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            PeerPair other = (PeerPair) obj;
            if (this.local == null) {
                if (other.local != null) {
                    return false;
                }
            } else if (!this.local.equals(other.local)) {
                return false;
            }
            if (this.remote == null) {
                if (other.remote != null) {
                    return false;
                }
            } else if (!this.remote.equals(other.remote)) {
                return false;
            }
            return true;
        }
    }

    static class PeerRequestContext {

        private final String                         groupId;
        private final PeerPair                       pair;

        // Executor to run the requests
        private SingleThreadExecutor                 executor;
        // The request sequence;
        private int                                  sequence;
        // The required sequence to be sent.
        private int                                  nextRequiredSequence;
        // The response queue,it's not thread-safe and protected by it self object monitor.
        private final PriorityQueue<SequenceMessage> responseQueue;

        private final int                            maxPendingResponses;

        public PeerRequestContext(final String groupId, final PeerPair pair, final int maxPendingResponses) {
            super();
            this.pair = pair;
            this.groupId = groupId;
            this.executor = new MpscSingleThreadExecutor(Utils.MAX_APPEND_ENTRIES_TASKS_PER_THREAD,
                JRaftUtils.createThreadFactory(groupId + "/" + pair + "-AppendEntriesThread"));

            this.sequence = 0;
            this.nextRequiredSequence = 0;
            this.maxPendingResponses = maxPendingResponses;
            this.responseQueue = new PriorityQueue<>(50);
        }

        boolean hasTooManyPendingResponses() {
            return this.responseQueue.size() > this.maxPendingResponses;
        }

        int getAndIncrementSequence() {
            final int prev = this.sequence;
            this.sequence++;
            if (this.sequence < 0) {
                this.sequence = 0;
            }
            return prev;
        }

        synchronized void destroy() {
            if (this.executor != null) {
                LOG.info("Destroyed peer request context for {}/{}", this.groupId, this.pair);
                this.executor.shutdownGracefully();
                this.executor = null;
            }
        }

        int getNextRequiredSequence() {
            return this.nextRequiredSequence;
        }

        int getAndIncrementNextRequiredSequence() {
            final int prev = this.nextRequiredSequence;
            this.nextRequiredSequence++;
            if (this.nextRequiredSequence < 0) {
                this.nextRequiredSequence = 0;
            }
            return prev;
        }
    }

    PeerRequestContext getPeerRequestContext(final String groupId, final PeerPair pair) {
        ConcurrentMap<PeerPair, PeerRequestContext> groupContexts = this.peerRequestContexts.get(groupId);

        if (groupContexts == null) {
            return null;
        }
        return groupContexts.get(pair);
    }

    /**
     * Send request in pipeline mode.
     */
    void sendSequenceResponse(final String groupId, final PeerPair pair, final int seq, final RpcContext rpcCtx,
                              final Message msg) {
        final PeerRequestContext ctx = getPeerRequestContext(groupId, pair);
        if (ctx == null) {
            // the context was destroyed, so the response can be ignored.
            return;
        }
        final PriorityQueue<SequenceMessage> respQueue = ctx.responseQueue;
        assert (respQueue != null);

        synchronized (Utils.withLockObject(respQueue)) {
            respQueue.add(new SequenceMessage(rpcCtx, msg, seq));

            if (!ctx.hasTooManyPendingResponses()) {
                while (!respQueue.isEmpty()) {
                    final SequenceMessage queuedPipelinedResponse = respQueue.peek();

                    if (queuedPipelinedResponse.sequence != ctx.getNextRequiredSequence()) {
                        // sequence mismatch, waiting for next response.
                        break;
                    }
                    respQueue.remove();
                    try {
                        queuedPipelinedResponse.sendResponse();
                    } finally {
                        ctx.getAndIncrementNextRequiredSequence();
                    }
                }
            } else {
                final Connection connection = rpcCtx.getConnection();
                LOG.warn("Closed connection to peer {}/{}, because of too many pending responses, queued={}, max={}",
                    ctx.groupId, pair, respQueue.size(), ctx.maxPendingResponses);
                connection.close();
                // Close the connection if there are too many pending responses in queue.
                removePeerRequestContext(groupId, pair);
            }
        }
    }

    @SuppressWarnings("unchecked")
    PeerRequestContext getOrCreatePeerRequestContext(final String groupId, final PeerPair pair, final Connection conn) {
        ConcurrentMap<PeerPair, PeerRequestContext> groupContexts = this.peerRequestContexts.get(groupId);
        if (groupContexts == null) {
            groupContexts = new ConcurrentHashMap<>();
            final ConcurrentMap<PeerPair, PeerRequestContext> existsCtxs = this.peerRequestContexts.putIfAbsent(
                groupId, groupContexts);
            if (existsCtxs != null) {
                groupContexts = existsCtxs;
            }
        }

        PeerRequestContext peerCtx = groupContexts.get(pair);
        if (peerCtx == null) {
            synchronized (Utils.withLockObject(groupContexts)) {
                peerCtx = groupContexts.get(pair);
                // double check in lock
                if (peerCtx == null) {
                    // only one thread to process append entries for every jraft node
                    final PeerId peer = new PeerId();
                    final boolean parsed = peer.parse(pair.local);
                    assert (parsed);
                    final Node node = NodeManager.getInstance().get(groupId, peer);
                    assert (node != null);
                    peerCtx = new PeerRequestContext(groupId, pair, node.getRaftOptions()
                        .getMaxReplicatorInflightMsgs());
                    groupContexts.put(pair, peerCtx);
                }
            }
        }

        // Add the pair to connection attribute metadata.
        if (conn != null) {
            Set<PeerPair> pairs;
            if ((pairs = (Set<AppendEntriesRequestProcessor.PeerPair>) conn.getAttribute(PAIR_ATTR)) == null) {
                pairs = new ConcurrentHashSet<>();
                Set<PeerPair> existsPairs = (Set<PeerPair>) conn.setAttributeIfAbsent(PAIR_ATTR, pairs);
                if (existsPairs != null) {
                    pairs = existsPairs;
                }
            }

            pairs.add(pair);
        }
        return peerCtx;
    }

    void removePeerRequestContext(final String groupId, final PeerPair pair) {
        final ConcurrentMap<PeerPair, PeerRequestContext> groupContexts = this.peerRequestContexts.get(groupId);
        if (groupContexts == null) {
            return;
        }
        synchronized (Utils.withLockObject(groupContexts)) {
            final PeerRequestContext ctx = groupContexts.remove(pair);
            if (ctx != null) {
                ctx.destroy();
            }
        }
    }

    /**
     * RAFT group peer request contexts.
     */
    private final ConcurrentMap<String /*groupId*/, ConcurrentMap<PeerPair, PeerRequestContext>> peerRequestContexts = new ConcurrentHashMap<>();

    /**
     * The executor selector to select executor for processing request.
     */
    private final ExecutorSelector                                                                executorSelector;

    public AppendEntriesRequestProcessor(final Executor executor) {
        super(executor, RpcRequests.AppendEntriesResponse.getDefaultInstance());
        this.executorSelector = new PeerExecutorSelector();
    }

    @Override
    protected String getPeerId(final AppendEntriesRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final AppendEntriesRequest request) {
        return request.getGroupId();
    }

    private int getAndIncrementSequence(final String groupId, final PeerPair pair, final Connection conn) {
        return getOrCreatePeerRequestContext(groupId, pair, conn).getAndIncrementSequence();
    }

    private boolean isHeartbeatRequest(final AppendEntriesRequest request) {
        // No entries and no data means a true heartbeat request.
        // TODO(boyan) refactor, adds a new flag field?
        return request.getEntriesCount() == 0 && !request.hasData();
    }

    @Override
    public Message processRequest0(final RaftServerService service, final AppendEntriesRequest request,
                                   final RpcRequestClosure done) {

        final Node node = (Node) service;

        if (node.getRaftOptions().isReplicatorPipeline()) {
            final String groupId = request.getGroupId();
            final PeerPair pair = pairOf(request.getPeerId(), request.getServerId());

            boolean isHeartbeat = isHeartbeatRequest(request);
            int reqSequence = -1;
            if (!isHeartbeat) {
                reqSequence = getAndIncrementSequence(groupId, pair, done.getRpcCtx().getConnection());
            }
            final Message response = service.handleAppendEntriesRequest(request, new SequenceRpcRequestClosure(done,
                defaultResp(), groupId, pair, reqSequence, isHeartbeat));
            if (response != null) {
                if (isHeartbeat) {
                    done.getRpcCtx().sendResponse(response);
                } else {
                    sendSequenceResponse(groupId, pair, reqSequence, done.getRpcCtx(), response);
                }
            }
            return null;
        } else {
            return service.handleAppendEntriesRequest(request, done);
        }
    }

    @Override
    public String interest() {
        return AppendEntriesRequest.class.getName();
    }

    @Override
    public ExecutorSelector executorSelector() {
        return this.executorSelector;
    }

    // TODO called when shutdown service.
    public void destroy() {
        for (final ConcurrentMap<PeerPair, PeerRequestContext> map : this.peerRequestContexts.values()) {
            for (final PeerRequestContext ctx : map.values()) {
                ctx.destroy();
            }
        }
        this.peerRequestContexts.clear();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onClosed(final String remoteAddress, final Connection conn) {
        final Set<PeerPair> pairs = (Set<PeerPair>) conn.getAttribute(PAIR_ATTR);
        if (pairs != null && !pairs.isEmpty()) {
            // Clear request contexts when connection disconnected.
            for (final Map.Entry<String, ConcurrentMap<PeerPair, PeerRequestContext>> entry : this.peerRequestContexts
                .entrySet()) {
                final ConcurrentMap<PeerPair, PeerRequestContext> groupCtxs = entry.getValue();
                synchronized (Utils.withLockObject(groupCtxs)) {
                    for (PeerPair pair : pairs) {
                        final PeerRequestContext ctx = groupCtxs.remove(pair);
                        if (ctx != null) {
                            ctx.destroy();
                        }
                    }
                }
            }
        } else {
            LOG.info("Connection disconnected: {}", remoteAddress);
        }
    }
}
