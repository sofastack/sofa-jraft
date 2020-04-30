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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.ErrorResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.PingRequest;
import com.alipay.sofa.jraft.test.MockAsyncContext;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.withSettings;

public class NodeRequestProcessorTest {

    private static class MockRequestProcessor extends NodeRequestProcessor<PingRequest> {

        private String peerId;
        private String groupId;

        public MockRequestProcessor(String peerId, String groupId) {
            super(null, null);
            this.peerId = peerId;
            this.groupId = groupId;
        }

        @Override
        protected String getPeerId(PingRequest request) {
            return this.peerId;
        }

        @Override
        protected String getGroupId(PingRequest request) {
            return this.groupId;
        }

        @Override
        protected Message processRequest0(RaftServerService serviceService, PingRequest request, RpcRequestClosure done) {
            return RpcFactoryHelper.responseFactory().newResponse(null, Status.OK());
        }

        @Override
        public String interest() {
            return PingRequest.class.getName();
        }

    }

    private MockRequestProcessor processor;
    private MockAsyncContext     asyncContext;

    @Before
    public void setup() {
        this.asyncContext = new MockAsyncContext();
        this.processor = new MockRequestProcessor("localhost:8081", "test");
    }

    @After
    public void teardown() {
        NodeManager.getInstance().clear();
    }

    @Test
    public void testOK() {
        Node node = Mockito.mock(Node.class, withSettings().extraInterfaces(RaftServerService.class));
        Mockito.when(node.getGroupId()).thenReturn("test");
        PeerId peerId = new PeerId("localhost", 8081);
        Mockito.when(node.getNodeId()).thenReturn(new NodeId("test", peerId));
        NodeManager.getInstance().addAddress(peerId.getEndpoint());
        NodeManager.getInstance().add(node);

        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(0, resp.getErrorCode());
    }

    @Test
    public void testInvalidPeerId() {
        this.processor = new MockRequestProcessor("localhost", "test");
        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(RaftError.EINVAL.getNumber(), resp.getErrorCode());
        assertEquals("Fail to parse peerId: localhost", resp.getErrorMsg());
    }

    @Test
    public void testPeerIdNotFound() {
        this.processor.handleRequest(asyncContext, TestUtils.createPingRequest());
        ErrorResponse resp = (ErrorResponse) asyncContext.getResponseObject();
        assertNotNull(resp);
        assertEquals(RaftError.ENOENT.getNumber(), resp.getErrorCode());
        assertEquals("Peer id not found: localhost:8081, group: test", resp.getErrorMsg());
    }
}
