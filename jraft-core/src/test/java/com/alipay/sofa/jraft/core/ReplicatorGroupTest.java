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

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
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

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
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

@RunWith(value = MockitoJUnitRunner.class)
public class ReplicatorGroupTest {

    static final Logger          LOG                    = LoggerFactory.getLogger(ReplicatorGroupTest.class);

    private TimerManager         timerManager;
    private ReplicatorGroup      replicatorGroup;
    @Mock
    private BallotBox            ballotBox;
    @Mock
    private LogManager           logManager;
    @Mock
    private NodeImpl             node;
    @Mock
    private RaftClientService    rpcService;
    @Mock
    private SnapshotStorage      snapshotStorage;
    private NodeOptions          options                = new NodeOptions();
    private final RaftOptions    raftOptions            = new RaftOptions();
    private final PeerId         peerId1                = new PeerId("localhost", 8082);
    private final PeerId         peerId2                = new PeerId("localhost", 8083);
    private final PeerId         peerId3                = new PeerId("localhost", 8084);
    private static AtomicInteger GLOBAL_ERROR_COUNTER   = new AtomicInteger(0);
    private static AtomicInteger GLOBAL_STOPED_COUNTER  = new AtomicInteger(0);
    private static AtomicInteger GLOBAL_STARTED_COUNTER = new AtomicInteger(0);

    @Before
    public void setup() {
        this.timerManager = new TimerManager();
        this.timerManager.init(5);
        this.replicatorGroup = new ReplicatorGroupImpl();
        final ReplicatorGroupOptions rgOpts = new ReplicatorGroupOptions();
        rgOpts.setHeartbeatTimeoutMs(heartbeatTimeout(this.options.getElectionTimeoutMs()));
        rgOpts.setElectionTimeoutMs(this.options.getElectionTimeoutMs());
        rgOpts.setLogManager(this.logManager);
        rgOpts.setBallotBox(this.ballotBox);
        rgOpts.setNode(this.node);
        rgOpts.setRaftRpcClientService(this.rpcService);
        rgOpts.setSnapshotStorage(this.snapshotStorage);
        rgOpts.setRaftOptions(this.raftOptions);
        rgOpts.setTimerManager(this.timerManager);
        Mockito.when(this.logManager.getLastLogIndex()).thenReturn(10L);
        Mockito.when(this.logManager.getTerm(10)).thenReturn(1L);
        Mockito.when(this.node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        Mockito.when(this.node.getNodeId()).thenReturn(new NodeId("test", new PeerId("localhost", 8081)));
        mockSendEmptyEntries();
        assertTrue(this.replicatorGroup.init(this.node.getNodeId(), rgOpts));
    }

    @Test
    public void testAddReplicatorAndFailed() {
        this.replicatorGroup.resetTerm(1);
        assertFalse(replicatorGroup.addReplicator(this.peerId1));
    }

    @Test
    public void testAddReplicatorSuccess() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        assertTrue(this.replicatorGroup.addReplicator(this.peerId1));
    }

    @Test
    public void testStopReplicator() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(this.peerId1);
        assertTrue(replicatorGroup.stopReplicator(this.peerId1));
    }

    @Test
    public void testStopAllReplicator() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(this.peerId1);
        this.replicatorGroup.addReplicator(this.peerId2);
        this.replicatorGroup.addReplicator(this.peerId3);
        assertTrue(this.replicatorGroup.contains(this.peerId1));
        assertTrue(this.replicatorGroup.contains(this.peerId2));
        assertTrue(this.replicatorGroup.contains(this.peerId3));
        assertTrue(this.replicatorGroup.stopAll());
    }

    @Test
    public void testReplicatorStoppedWithRepliactorListener() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(this.peerId1);
        this.replicatorGroup.addReplicator(this.peerId2);
        this.replicatorGroup.addReplicator(this.peerId3);
        Mockito.when(this.node.getReplicatorListener()).thenReturn(new UserReplicatorStateListener());
        assertTrue(this.replicatorGroup.stopAll());
        assertEquals(3, GLOBAL_STOPED_COUNTER.get());
    }

    @Test
    public void testReplicatorErrorWithRepliactorListener() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(this.peerId1);
        this.replicatorGroup.addReplicator(this.peerId2);
        this.replicatorGroup.addReplicator(this.peerId3);
        Mockito.when(this.node.getReplicatorListener()).thenReturn(new UserReplicatorStateListener());

        // 1.mock replicator Heartbeat return error status
        final ThreadId id = replicatorGroup.getReplicator(peerId1);
        Replicator.onHeartbeatReturned(id, new Status(-1, "test"), this.createEmptyEntriesRequestToPeer(peerId1), null,
            Utils.monotonicMs());
        assertEquals(1, GLOBAL_ERROR_COUNTER.get());

        // 2.mock replicator rpc return error status
        final RpcRequests.AppendEntriesResponse response = RpcRequests.AppendEntriesResponse.newBuilder() //
            .setSuccess(false) //
            .setLastLogIndex(10) //
            .setTerm(1) //
            .build();
        Replicator.onRpcReturned(id, Replicator.RequestType.AppendEntries, new Status(-1, "test error"),
            this.createEmptyEntriesRequestToPeer(peerId1), response, 0, 0, Utils.monotonicMs());
        assertEquals(2, GLOBAL_ERROR_COUNTER.get());
        assertTrue(this.replicatorGroup.stopAll());
    }

    @Test
    public void testReplicatorStartededWithRepliactorListener() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        Mockito.when(this.node.getReplicatorListener()).thenReturn(new UserReplicatorStateListener());
        this.replicatorGroup.addReplicator(this.peerId1);
        this.replicatorGroup.addReplicator(this.peerId2);
        this.replicatorGroup.addReplicator(this.peerId3);
        assertTrue(this.replicatorGroup.stopAll());
        assertEquals(3, GLOBAL_STARTED_COUNTER.get());
    }

    @Test
    public void testRemoveRepliactorListenerExcetpionFromRepliactor() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        Mockito.when(this.node.getReplicatorListener()).thenReturn(new UserReplicatorStateListener());
        this.replicatorGroup.addReplicator(this.peerId1);
        this.replicatorGroup.addReplicator(this.peerId2);
        this.replicatorGroup.addReplicator(this.peerId3);
        Mockito.when(this.node.getReplicatorListener()).thenReturn(null);
        assertTrue(this.replicatorGroup.stopAll());
        assertEquals(0, GLOBAL_STOPED_COUNTER.get());
    }

    static class UserReplicatorStateListener implements Replicator.ReplicatorStateListener {
        @Override
        public void onStarted() {
            LOG.info("Replicator has started");
            GLOBAL_STARTED_COUNTER.incrementAndGet();
        }

        @Override
        public void onError(Status status) {
            LOG.info("Replicator has errors");
            GLOBAL_ERROR_COUNTER.incrementAndGet();
        }

        @Override
        public void onStoped() {
            LOG.info("Replicator has stopped");
            GLOBAL_STOPED_COUNTER.incrementAndGet();
        }
    }

    @Test
    public void testTransferLeadershipToAndStop() {
        Mockito.when(this.rpcService.connect(this.peerId1.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId2.getEndpoint())).thenReturn(true);
        Mockito.when(this.rpcService.connect(this.peerId3.getEndpoint())).thenReturn(true);
        this.replicatorGroup.resetTerm(1);
        this.replicatorGroup.addReplicator(this.peerId1);
        this.replicatorGroup.addReplicator(this.peerId2);
        this.replicatorGroup.addReplicator(this.peerId3);
        long logIndex = 8;
        assertTrue(this.replicatorGroup.transferLeadershipTo(this.peerId1, 8));
        final Replicator r = (Replicator) this.replicatorGroup.getReplicator(this.peerId1).lock();
        assertEquals(r.getTimeoutNowIndex(), logIndex);
        this.replicatorGroup.getReplicator(this.peerId1).unlock();
        assertTrue(this.replicatorGroup.stopTransferLeadership(this.peerId1));
        assertEquals(r.getTimeoutNowIndex(), 0);
    }

    @After
    public void teardown() {
        this.timerManager.shutdown();
        this.GLOBAL_ERROR_COUNTER.set(0);
        this.GLOBAL_STOPED_COUNTER.set(0);
        this.GLOBAL_STARTED_COUNTER.set(0);
    }

    private int heartbeatTimeout(final int electionTimeout) {
        return Math.max(electionTimeout / this.raftOptions.getElectionHeartbeatFactor(), 10);
    }

    private void mockSendEmptyEntries() {
        final RpcRequests.AppendEntriesRequest request1 = createEmptyEntriesRequestToPeer(this.peerId1);
        final RpcRequests.AppendEntriesRequest request2 = createEmptyEntriesRequestToPeer(this.peerId2);
        final RpcRequests.AppendEntriesRequest request3 = createEmptyEntriesRequestToPeer(this.peerId3);

        Mockito
            .when(this.rpcService.appendEntries(eq(this.peerId1.getEndpoint()), eq(request1), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());
        Mockito
            .when(this.rpcService.appendEntries(eq(this.peerId2.getEndpoint()), eq(request2), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());
        Mockito
            .when(this.rpcService.appendEntries(eq(this.peerId3.getEndpoint()), eq(request3), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());
    }

    private RpcRequests.AppendEntriesRequest createEmptyEntriesRequestToPeer(final PeerId peerId) {
        return RpcRequests.AppendEntriesRequest.newBuilder() //
            .setGroupId("test") //
            .setServerId(new PeerId("localhost", 8081).toString()) //
            .setPeerId(peerId.toString()) //
            .setTerm(1) //
            .setPrevLogIndex(10) //
            .setPrevLogTerm(1) //
            .setCommittedIndex(0) //
            .build();
    }

}
