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

import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.commons.lang.StringUtils;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.Connection;
import com.alipay.remoting.ConnectionEventProcessor;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequestHeader;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.MpscSingleThreadExecutor;
import com.alipay.sofa.jraft.util.concurrent.SingleThreadExecutor;
import com.google.protobuf.Message;

/**
 * Append entries request processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 3:00:13 PM
 */
public class AppendEntriesRequestProcessor extends NodeRequestProcessor<AppendEntriesRequest> implements
                                                                                             ConnectionEventProcessor {

    static final String PEER_ATTR = "jraft-peer";

    /**
     * Peer executor selector.
     * @author dennis
     */
    final class PeerExecutorSelector implements ExecutorSelector {

        PeerExecutorSelector() {
            super();
        }

        @Override
        public Executor select(final String requestClass, final Object requestHeader) {
            final AppendEntriesRequestHeader header = (AppendEntriesRequestHeader) requestHeader;
            final String groupId = header.getGroupId();
            final String peerId = header.getPeerId();

            final PeerId peer = new PeerId();

            if (!peer.parse(peerId)) {
                return getExecutor();
            }

            final Node node = NodeManager.getInstance().get(groupId, peer);

            if (node == null || !node.getRaftOptions().isReplicatorPipeline()) {
                return getExecutor();
            }

            // The node enable pipeline, we should ensure bolt support it.
            Utils.ensureBoltPipeline();

            final PeerRequestContext ctx = getPeerRequestContext(groupId, peerId, null);

            return ctx.executor;
        }
    }

    /**
     * RpcRequestClosure that will send responses in pipeline mode.
     *
     * @author dennis
     */
    class SequenceRpcRequestClosure extends RpcRequestClosure {

        private final int    reqSequence;
        private final String groupId;
        private final String peerId;

        public SequenceRpcRequestClosure(RpcRequestClosure parent, int sequence, String groupId, String peerId) {
            super(parent.getBizContext(), parent.getAsyncContext());
            this.reqSequence = sequence;
            this.groupId = groupId;
            this.peerId = peerId;
        }

        @Override
        public void sendResponse(final Message msg) {
            sendSequenceResponse(this.groupId, this.peerId, this.reqSequence, getAsyncContext(), getBizContext(), msg);
        }
    }

    /**
     * Response message wrapper with a request sequence number and asyncContext.done
     * @author dennis
     */
    static class SequenceMessage implements Comparable<SequenceMessage> {
        public final Message       msg;
        private final int          sequence;
        private final AsyncContext asyncContext;

        public SequenceMessage(AsyncContext asyncContext, Message msg, int sequence) {
            super();
            this.asyncContext = asyncContext;
            this.msg = msg;
            this.sequence = sequence;
        }

        /**
         * Send the response.
         */
        void sendResponse() {
            this.asyncContext.sendResponse(this.msg);
        }

        /**
         * Order by sequence number
         */
        @Override
        public int compareTo(final SequenceMessage o) {
            return Integer.compare(this.sequence, o.sequence);
        }
    }

    /**
     * Send request in pipeline mode.
     */
    void sendSequenceResponse(final String groupId, final String peerId, final int seq,
                              final AsyncContext asyncContext, final BizContext bizContext, final Message msg) {
        final Connection connection = bizContext.getConnection();
        final PeerRequestContext ctx = getPeerRequestContext(groupId, peerId, connection);
        final PriorityQueue<SequenceMessage> respQueue = ctx.responseQueue;
        assert (respQueue != null);

        synchronized (Utils.withLockObject(respQueue)) {
            respQueue.add(new SequenceMessage(asyncContext, msg, seq));

            if (!ctx.hasTooManyPendingResponses()) {
                while (!respQueue.isEmpty()) {
                    final SequenceMessage queuedPipelinedResponse = respQueue.peek();

                    if (queuedPipelinedResponse.sequence != getNextRequiredSequence(groupId, peerId, connection)) {
                        // sequence mismatch, waiting for next response.
                        break;
                    }
                    respQueue.remove();
                    try {
                        queuedPipelinedResponse.sendResponse();
                    } finally {
                        getAndIncrementNextRequiredSequence(groupId, peerId, connection);
                    }
                }
            } else {
                LOG.warn("Closed connection to peer {}/{}, because of too many pending responses, queued={}, max={}",
                    ctx.groupId, peerId, respQueue.size(), ctx.maxPendingResponses);
                connection.close();
                // Close the connection if there are too many pending responses in queue.
                removePeerRequestContext(groupId, peerId);
            }
        }
    }

    static class PeerRequestContext {

        private final String                         groupId;
        private final String                         peerId;

        // Executor to run the requests
        private SingleThreadExecutor                 executor;
        // The request sequence;
        private int                                  sequence;
        // The required sequence to be sent.
        private int                                  nextRequiredSequence;
        // The response queue,it's not thread-safe and protected by it self object monitor.
        private final PriorityQueue<SequenceMessage> responseQueue;

        private final int                            maxPendingResponses;

        public PeerRequestContext(final String groupId, final String peerId, final int maxPendingResponses) {
            super();
            this.peerId = peerId;
            this.groupId = groupId;
            this.executor = new MpscSingleThreadExecutor(Utils.MAX_APPEND_ENTRIES_TASKS_PER_THREAD,
                JRaftUtils.createThreadFactory(groupId + "/" + peerId + "-AppendEntriesThread"));

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
                LOG.info("Destroyed peer request context for {}/{}", this.groupId, this.peerId);
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

    PeerRequestContext getPeerRequestContext(final String groupId, final String peerId, final Connection conn) {
        ConcurrentMap<String/* peerId */, PeerRequestContext> groupContexts = this.peerRequestContexts.get(groupId);
        if (groupContexts == null) {
            groupContexts = new ConcurrentHashMap<>();
            final ConcurrentMap<String, PeerRequestContext> existsCtxs = this.peerRequestContexts.putIfAbsent(groupId,
                groupContexts);
            if (existsCtxs != null) {
                groupContexts = existsCtxs;
            }
        }

        PeerRequestContext peerCtx = groupContexts.get(peerId);
        if (peerCtx == null) {
            synchronized (Utils.withLockObject(groupContexts)) {
                peerCtx = groupContexts.get(peerId);
                // double check in lock
                if (peerCtx == null) {
                    // only one thread to process append entries for every jraft node
                    final PeerId peer = new PeerId();
                    final boolean parsed = peer.parse(peerId);
                    assert (parsed);
                    final Node node = NodeManager.getInstance().get(groupId, peer);
                    assert (node != null);
                    peerCtx = new PeerRequestContext(groupId, peerId, node.getRaftOptions()
                        .getMaxReplicatorInflightMsgs());
                    groupContexts.put(peerId, peerCtx);
                }
            }
        }
        // Set peer attribute into connection if absent
        if (conn != null && conn.getAttribute(PEER_ATTR) == null) {
            conn.setAttribute(PEER_ATTR, peerId);
        }
        return peerCtx;
    }

    void removePeerRequestContext(final String groupId, final String peerId) {
        final ConcurrentMap<String/* peerId */, PeerRequestContext> groupContexts = this.peerRequestContexts
            .get(groupId);
        if (groupContexts == null) {
            return;
        }
        synchronized (Utils.withLockObject(groupContexts)) {
            final PeerRequestContext ctx = groupContexts.remove(peerId);
            if (ctx != null) {
                ctx.destroy();
            }
        }
    }

    /**
     * RAFT group peer request contexts
     * Map<groupId, <peerId, ctx>>
     */
    private final ConcurrentMap<String, ConcurrentMap<String, PeerRequestContext>> peerRequestContexts = new ConcurrentHashMap<>();

    /**
     * The executor selector to select executor for processing request.
     */
    private final ExecutorSelector                                                 executorSelector;

    public AppendEntriesRequestProcessor(Executor executor) {
        super(executor);
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

    private int getAndIncrementSequence(final String groupId, final String peerId, final Connection conn) {
        return getPeerRequestContext(groupId, peerId, conn).getAndIncrementSequence();
    }

    private int getNextRequiredSequence(final String groupId, final String peerId, final Connection conn) {
        return getPeerRequestContext(groupId, peerId, conn).getNextRequiredSequence();
    }

    private int getAndIncrementNextRequiredSequence(final String groupId, final String peerId, final Connection conn) {
        return getPeerRequestContext(groupId, peerId, conn).getAndIncrementNextRequiredSequence();
    }

    @Override
    public Message processRequest0(final RaftServerService service, final AppendEntriesRequest request,
                                   final RpcRequestClosure done) {

        final Node node = (Node) service;

        if (node.getRaftOptions().isReplicatorPipeline()) {
            final String groupId = request.getGroupId();
            final String peerId = request.getPeerId();

            final int reqSequence = getAndIncrementSequence(groupId, peerId, done.getBizContext().getConnection());
            final Message response = service.handleAppendEntriesRequest(request, new SequenceRpcRequestClosure(done,
                reqSequence, groupId, peerId));
            if (response != null) {
                sendSequenceResponse(groupId, peerId, reqSequence, done.getAsyncContext(), done.getBizContext(),
                    response);
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
    public ExecutorSelector getExecutorSelector() {
        return this.executorSelector;
    }

    // TODO called when shutdown service.
    public void destroy() {
        for (final ConcurrentMap<String/* peerId */, PeerRequestContext> map : this.peerRequestContexts.values()) {
            for (final PeerRequestContext ctx : map.values()) {
                ctx.destroy();
            }
        }
    }

    @Override
    public void onEvent(final String remoteAddr, final Connection conn) {
        final PeerId peer = new PeerId();
        final String peerAttr = (String) conn.getAttribute(PEER_ATTR);

        if (!StringUtils.isBlank(peerAttr) && peer.parse(peerAttr)) {
            // Clear request context when connection disconnected.
            for (final Map.Entry<String, ConcurrentMap<String, PeerRequestContext>> entry : this.peerRequestContexts
                .entrySet()) {
                final ConcurrentMap<String, PeerRequestContext> groupCtxs = entry.getValue();
                synchronized (Utils.withLockObject(groupCtxs)) {
                    final PeerRequestContext ctx = groupCtxs.remove(peer.toString());
                    if (ctx != null) {
                        ctx.destroy();
                    }
                }
            }
        } else {
            LOG.info("Connection disconnected: {}", remoteAddr);
        }
    }
}
