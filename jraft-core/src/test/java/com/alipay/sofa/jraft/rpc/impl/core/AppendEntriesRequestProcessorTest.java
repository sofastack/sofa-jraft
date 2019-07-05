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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.Connection;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.PingRequest;
import com.alipay.sofa.jraft.rpc.impl.core.AppendEntriesRequestProcessor.PeerRequestContext;
import com.alipay.sofa.jraft.test.TestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.eq;

public class AppendEntriesRequestProcessorTest extends BaseNodeRequestProcessorTest<AppendEntriesRequest> {

    private AppendEntriesRequest request;

    @Override
    public AppendEntriesRequest createRequest(String groupId, PeerId peerId) {
        request = AppendEntriesRequest.newBuilder().setCommittedIndex(0). //
            setGroupId(groupId). //
            setPeerId(peerId.toString()).//
            setServerId("localhost:8082"). //
            setPrevLogIndex(0). //
            setTerm(0). //
            setPrevLogTerm(0).build();
        return request;
    }

    @Mock
    private Connection conn;

    @Override
    public void setup() {
        super.setup();
        Mockito.when(this.bizContext.getConnection()).thenReturn(this.conn);
        Mockito.when(this.conn.getAttribute(AppendEntriesRequestProcessor.PEER_ATTR)).thenReturn(this.peerIdStr);
    }

    private ExecutorService executor;

    @Override
    public NodeRequestProcessor<AppendEntriesRequest> newProcessor() {
        executor = Executors.newSingleThreadExecutor();
        return new AppendEntriesRequestProcessor(executor);
    }

    @Override
    public void teardown() {
        super.teardown();
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public void verify(String interest, RaftServerService service, NodeRequestProcessor<AppendEntriesRequest> processor) {
        assertEquals(interest, AppendEntriesRequest.class.getName());
        Mockito.verify(service).handleAppendEntriesRequest(eq(request), Mockito.any());
        final PeerRequestContext ctx = ((AppendEntriesRequestProcessor) processor).getPeerRequestContext(groupId,
            peerIdStr, conn);
        assertNotNull(ctx);
    }

    @Test
    public void testGetPeerRequestContextRemovePeerRequestContext() {
        mockNode();

        final AppendEntriesRequestProcessor processor = (AppendEntriesRequestProcessor) newProcessor();
        final PeerRequestContext ctx = processor.getPeerRequestContext(groupId, peerIdStr, conn);
        assertNotNull(ctx);
        assertSame(ctx, processor.getPeerRequestContext(groupId, peerIdStr, conn));
        assertEquals(0, ctx.getNextRequiredSequence());
        assertEquals(0, ctx.getAndIncrementSequence());
        assertEquals(1, ctx.getAndIncrementSequence());
        assertEquals(0, ctx.getAndIncrementNextRequiredSequence());
        assertEquals(1, ctx.getAndIncrementNextRequiredSequence());
        assertFalse(ctx.hasTooManyPendingResponses());

        processor.removePeerRequestContext(groupId, peerIdStr);
        final PeerRequestContext newCtx = processor.getPeerRequestContext(groupId, peerIdStr, conn);
        assertNotNull(newCtx);
        assertNotSame(ctx, newCtx);

        assertEquals(0, newCtx.getNextRequiredSequence());
        assertEquals(0, newCtx.getAndIncrementSequence());
        assertEquals(1, newCtx.getAndIncrementSequence());
        assertEquals(0, newCtx.getAndIncrementNextRequiredSequence());
        assertEquals(1, newCtx.getAndIncrementNextRequiredSequence());
        assertFalse(newCtx.hasTooManyPendingResponses());
    }

    @Test
    public void testSendSequenceResponse() {
        mockNode();

        final AsyncContext asyncContext = Mockito.mock(AsyncContext.class);
        final BizContext bizContext = Mockito.mock(BizContext.class);
        final AppendEntriesRequestProcessor processor = (AppendEntriesRequestProcessor) newProcessor();
        final PingRequest msg = TestUtils.createPingRequest();
        processor.sendSequenceResponse(groupId, peerIdStr, 1, asyncContext, bizContext, msg);
        Mockito.verify(asyncContext, Mockito.never()).sendResponse(msg);

        processor.sendSequenceResponse(groupId, peerIdStr, 0, asyncContext, bizContext, msg);
        Mockito.verify(asyncContext, Mockito.times(2)).sendResponse(msg);
    }

    @Test
    public void testTooManyPendingResponses() {
        final PeerId peer = this.mockNode();
        NodeManager.getInstance().get(groupId, peer).getRaftOptions().setMaxReplicatorInflightMsgs(2);

        final AsyncContext asyncContext = Mockito.mock(AsyncContext.class);
        final BizContext bizContext = Mockito.mock(BizContext.class);
        final AppendEntriesRequestProcessor processor = (AppendEntriesRequestProcessor) newProcessor();
        final PingRequest msg = TestUtils.createPingRequest();
        final Connection conn = Mockito.mock(Connection.class);
        Mockito.when(bizContext.getConnection()).thenReturn(conn);
        final PeerRequestContext ctx = processor.getPeerRequestContext(groupId, peerIdStr, conn);
        assertNotNull(ctx);
        processor.sendSequenceResponse(groupId, peerIdStr, 1, asyncContext, bizContext, msg);
        processor.sendSequenceResponse(groupId, peerIdStr, 2, asyncContext, bizContext, msg);
        processor.sendSequenceResponse(groupId, peerIdStr, 3, asyncContext, bizContext, msg);
        Mockito.verify(asyncContext, Mockito.never()).sendResponse(msg);
        Mockito.verify(conn).close();

        final PeerRequestContext newCtx = processor.getPeerRequestContext(groupId, peerIdStr, conn);
        assertNotNull(newCtx);
        assertNotSame(ctx, newCtx);
    }

}
