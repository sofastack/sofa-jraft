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
package com.alipay.sofa.jraft.rpc.impl.cli;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alipay.sofa.jraft.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.test.MockAsyncContext;
import com.google.protobuf.Message;

@RunWith(value = MockitoJUnitRunner.class)
public abstract class AbstractCliRequestProcessorTest<T extends Message> {
    @Mock
    private Node               node;
    private final String       groupId   = "test";
    private final String       peerIdStr = "localhost:8081";
    protected MockAsyncContext asyncContext;

    public abstract T createRequest(String groupId, PeerId peerId);

    public abstract BaseCliRequestProcessor<T> newProcessor();

    public abstract void verify(String interest, Node node, ArgumentCaptor<Closure> doneArg);

    public void mockNodes(final int n) {
        ArrayList<PeerId> peers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            peers.add(JRaftUtils.getPeerId("localhost:" + (8081 + i)));
        }
        Map<PeerId, PeerId> learners = new ConcurrentHashMap<>();
        for (int i = 0; i < n; i++) {
            learners.put(JRaftUtils.getPeerId("learner:" + (8081 + i)), Configuration.NULL_PEERID);
        }
        Mockito.when(this.node.listPeers()).thenReturn(peers);
        Mockito.when(this.node.listLearners()).thenReturn(learners);
    }

    @Before
    public void setup() {
        this.asyncContext = new MockAsyncContext();
    }

    @After
    public void teardown() {
        NodeManager.getInstance().clear();
    }

    @Test
    public void testHandleRequest() {
        this.mockNodes(3);
        Mockito.when(this.node.getGroupId()).thenReturn(this.groupId);
        PeerId peerId = new PeerId();
        peerId.parse(this.peerIdStr);
        Mockito.when(this.node.getOptions()).thenReturn(new NodeOptions());
        Mockito.when(this.node.getNodeId()).thenReturn(new NodeId("test", peerId));
        NodeManager.getInstance().addAddress(peerId.getEndpoint());
        NodeManager.getInstance().add(this.node);

        BaseCliRequestProcessor<T> processor = newProcessor();
        processor.handleRequest(this.asyncContext, createRequest(this.groupId, peerId));
        ArgumentCaptor<Closure> doneArg = ArgumentCaptor.forClass(Closure.class);
        verify(processor.interest(), this.node, doneArg);
    }
}
