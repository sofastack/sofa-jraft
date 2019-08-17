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

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReplicatorGroupOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.impl.FutureImpl;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.Endpoint;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;

@RunWith(value = MockitoJUnitRunner.class)
public class ReplicatorGroupTest {

    private TimerManager           timerManager;
    private ReplicatorGroup        replicatorGroup;
    @Mock
    private BallotBox              ballotBox;
    @Mock
    private LogManager             logManager;
    @Mock
    private NodeImpl               node;
    private ReplicatorGroupOptions rgOpts;
    @Mock
    private RaftClientService      rpcService;
    @Mock
    private SnapshotStorage        snapshotStorage;
    private NodeOptions            options     = new NodeOptions();
    private final RaftOptions      raftOptions = new RaftOptions();
    private final PeerId           peerId1     = new PeerId("localhost", 8082);
    private final PeerId           peerId2     = new PeerId("localhost", 8083);
    private final PeerId           peerId3     = new PeerId("localhost", 8084);

    @Before
    public void setup() {
        this.timerManager = new TimerManager();
        this.timerManager.init(5);
        replicatorGroup = new ReplicatorGroupImpl();
        rgOpts = new ReplicatorGroupOptions();
        rgOpts.setHeartbeatTimeoutMs(heartbeatTimeout(this.options.getElectionTimeoutMs()));
        rgOpts.setElectionTimeoutMs(this.options.getElectionTimeoutMs());
        rgOpts.setLogManager(this.logManager);
        rgOpts.setBallotBox(this.ballotBox);
        rgOpts.setNode(node);
        rgOpts.setRaftRpcClientService(this.rpcService);
        rgOpts.setSnapshotStorage(snapshotStorage);
        rgOpts.setRaftOptions(this.raftOptions);
        rgOpts.setTimerManager(this.timerManager);
        Mockito.when(this.logManager.getLastLogIndex()).thenReturn(10L);
        Mockito.when(this.logManager.getTerm(10)).thenReturn(1L);
        Mockito.when(this.node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        Mockito.when(this.node.getNodeId()).thenReturn(new NodeId("test", new PeerId("localhost", 8081)));
        mockSendEmptyEntries();
        assertTrue(this.replicatorGroup.init(node.getNodeId(), rgOpts));
    }

    @Test
    public void testAddReplicatorAndFailed() {
        replicatorGroup.resetTerm(1);
        assertFalse(replicatorGroup.addReplicator(peerId1));
    }

    @Test
    public void testAddReplicatorSuccess() {
        Mockito.when(this.rpcService.connect(peerId1.getEndpoint())).thenReturn(true);
        replicatorGroup.resetTerm(1);
        assertTrue(replicatorGroup.addReplicator(peerId1));
    }

    @Test
    public void testStopReplicator() {
        Mockito.when(this.rpcService.connect(peerId1.getEndpoint())).thenReturn(true);
        replicatorGroup.resetTerm(1);
        replicatorGroup.addReplicator(peerId1);
        assertTrue(replicatorGroup.stopReplicator(peerId1));
    }

    @Test
    public void testStopAllReplicator() {
        Mockito.when(this.rpcService.connect(peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(peerId3.getEndpoint())).thenReturn(true);
        replicatorGroup.resetTerm(1);
        replicatorGroup.addReplicator(peerId1);
        replicatorGroup.addReplicator(peerId2);
        replicatorGroup.addReplicator(peerId3);
        assertTrue(replicatorGroup.contains(peerId1));
        assertTrue(replicatorGroup.contains(peerId2));
        assertTrue(replicatorGroup.contains(peerId3));
        assertTrue(replicatorGroup.stopAll());
    }

    @Test
    public void testTransferLeadershipToAndStop() {
        Mockito.when(this.rpcService.connect(peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(peerId3.getEndpoint())).thenReturn(true);
        replicatorGroup.resetTerm(1);
        replicatorGroup.addReplicator(peerId1);
        replicatorGroup.addReplicator(peerId2);
        replicatorGroup.addReplicator(peerId3);
        long logIndx = 8;
        assertTrue(replicatorGroup.transferLeadershipTo(peerId1, 8));
        Replicator r = (Replicator) replicatorGroup.getReplicator(peerId1).lock();
        assertEquals(r.getTimeoutNowIndex(), logIndx);
        replicatorGroup.getReplicator(peerId1).unlock();
        assertTrue(replicatorGroup.stopTransferLeadership(peerId1));
        assertEquals(r.getTimeoutNowIndex(), 0);
    }

    @After
    public void teardown() {
        this.timerManager.shutdown();
    }

    private int heartbeatTimeout(final int electionTimeout) {
        return Math.max(electionTimeout / this.raftOptions.getElectionHeartbeatFactor(), 10);
    }

    private void mockSendEmptyEntries() {
        final RpcRequests.AppendEntriesRequest request1 = createEmptyEntriesRequestToPeer1();
        final RpcRequests.AppendEntriesRequest request2 = createEmptyEntriesRequestToPeer2();
        final RpcRequests.AppendEntriesRequest request3 = createEmptyEntriesRequestToPeer3();

        Mockito.when(this.rpcService.appendEntries(eq(peerId1.getEndpoint()), eq(request1), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());
        Mockito.when(this.rpcService.appendEntries(eq(peerId2.getEndpoint()), eq(request2), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());
        Mockito.when(this.rpcService.appendEntries(eq(peerId3.getEndpoint()), eq(request3), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());
    }

    private RpcRequests.AppendEntriesRequest createEmptyEntriesRequestToPeer1() {
        final RpcRequests.AppendEntriesRequest request = RpcRequests.AppendEntriesRequest.newBuilder(). //
            setGroupId("test"). //
            setServerId(new PeerId("localhost", 8081).toString()). //
            setPeerId(this.peerId1.toString()). //
            setTerm(1). //
            setPrevLogIndex(10). //
            setPrevLogTerm(1). //
            setCommittedIndex(0).build();
        return request;
    }

    private RpcRequests.AppendEntriesRequest createEmptyEntriesRequestToPeer2() {
        final RpcRequests.AppendEntriesRequest request = RpcRequests.AppendEntriesRequest.newBuilder(). //
            setGroupId("test"). //
            setServerId(new PeerId("localhost", 8081).toString()). //
            setPeerId(this.peerId2.toString()). //
            setTerm(1). //
            setPrevLogIndex(10). //
            setPrevLogTerm(1). //
            setCommittedIndex(0).build();
        return request;
    }

    private RpcRequests.AppendEntriesRequest createEmptyEntriesRequestToPeer3() {
        final RpcRequests.AppendEntriesRequest request = RpcRequests.AppendEntriesRequest.newBuilder(). //
            setGroupId("test"). //
            setServerId(new PeerId("localhost", 8081).toString()). //
            setPeerId(this.peerId3.toString()). //
            setTerm(1). //
            setPrevLogIndex(10). //
            setPrevLogTerm(1). //
            setCommittedIndex(0).build();
        return request;
    }
}
