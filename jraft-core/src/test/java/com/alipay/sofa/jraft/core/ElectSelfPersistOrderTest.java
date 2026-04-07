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

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.impl.LocalRaftMetaStorage;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Test that electSelf() persists (term, votedFor) BEFORE sending RequestVote RPCs.
 * <p>
 * Before the fix, electSelf() sent RequestVote RPCs to all peers in a loop and
 * only then called metaStorage.setTermAndVotedFor(). If the node crashed between
 * sending the RPCs and persisting, on restart it would not remember its own vote,
 * allowing it to vote for a different candidate in the same term — a Raft safety
 * violation (Section 5.2 of the Raft paper).
 * <p>
 * The fix moves setTermAndVotedFor() before the RPC loop and checks its return
 * value. On failure, the node steps down without sending any RPCs.
 */
public class ElectSelfPersistOrderTest {

    private String dataPath;

    @Before
    public void setup() throws Exception {
        this.dataPath = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.dataPath));
    }

    @After
    public void teardown() throws Exception {
        if (!TestCluster.CLUSTERS.isEmpty()) {
            for (final TestCluster c : TestCluster.CLUSTERS.removeAll()) {
                try {
                    c.stopAll();
                } catch (final Exception ignored) {
                }
            }
        }
        FileUtils.deleteDirectory(new File(this.dataPath));
    }

    /**
     * Verify that when setTermAndVotedFor() fails during electSelf(), the node
     * steps down and does NOT send RequestVote RPCs to peers.
     * <p>
     * Approach:
     * 1. Start a 3-node cluster with a FailingMetaStorage on node 0
     * 2. Wait for leader election to complete normally (fail flag is off)
     * 3. Ensure node 0 is NOT the leader
     * 4. Enable the failure flag on node 0's meta storage
     * 5. Directly trigger electSelf() on node 0 via tryElectSelf()
     * 6. electSelf() increments term, calls setTermAndVotedFor() which fails,
     * then steps down WITHOUT sending any RequestVote RPCs
     * 7. Verify the observer node's term did NOT advance (proving no RPC was sent)
     * <p>
     * Using tryElectSelf() instead of killing the leader avoids a timing race:
     * killing the leader requires waiting ~10s for the observer's leader lease
     * to expire before node 0's preVote can succeed, but the observer's own
     * election timer (~10s) fires at the same time, causing the observer to
     * start its own election and bump its term independently of node 0.
     */
    @Test
    public void testPersistFailurePreventsRpcSend() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);
        final String groupId = "electself-test";
        final Configuration conf = new Configuration(peers);

        final AtomicBoolean failFlag = new AtomicBoolean(false);
        final AtomicLong failedAtTerm = new AtomicLong(-1);
        final CountDownLatch failLatch = new CountDownLatch(1);

        final List<RaftGroupService> services = new ArrayList<>();
        final List<Node> nodes = new ArrayList<>();

        for (int i = 0; i < peers.size(); i++) {
            final PeerId peer = peers.get(i);
            final NodeOptions nodeOptions = new NodeOptions();
            nodeOptions.setElectionTimeoutMs(i == 0 ? 300 : 10_000);
            nodeOptions.setSnapshotIntervalSecs(300);
            nodeOptions.setInitialConf(conf);

            final String serverDataPath = this.dataPath + File.separator
                                          + peer.getEndpoint().toString().replace(':', '_');
            FileUtils.forceMkdir(new File(serverDataPath));
            nodeOptions.setLogUri(serverDataPath + File.separator + "logs");
            nodeOptions.setRaftMetaUri(serverDataPath + File.separator + "meta");
            nodeOptions.setSnapshotUri(serverDataPath + File.separator + "snapshot");
            nodeOptions.setFsm(new StateMachineAdapter() {
                @Override
                public void onApply(final Iterator iter) {
                    while (iter.hasNext()) {
                        iter.next();
                    }
                }
            });

            if (i == 0) {
                nodeOptions.setServiceFactory(new FailingServiceFactory(failFlag, failedAtTerm, failLatch));
            }

            final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(peer.getEndpoint());
            final RaftGroupService svc = new RaftGroupService(groupId, peer, nodeOptions, rpcServer);
            final Node node = svc.start();
            assertNotNull(node);
            services.add(svc);
            nodes.add(node);
        }

        try {
            // Wait for initial leader election (fail flag is off)
            Thread.sleep(3000);

            Node leader = null;
            for (final Node n : nodes) {
                if (n.getLeaderId() != null && !n.getLeaderId().isEmpty()) {
                    if (n.getNodeId().getPeerId().equals(n.getLeaderId())) {
                        leader = n;
                    }
                }
            }
            assertNotNull("Should have a leader", leader);

            final Node node0 = nodes.get(0);

            // If node0 is the leader, transfer leadership away
            if (leader == node0) {
                leader.transferLeadershipTo(peers.get(1));
                Thread.sleep(2000);
                leader = null;
                for (final Node n : nodes) {
                    if (n.getLeaderId() != null && !n.getLeaderId().isEmpty()) {
                        if (n.getNodeId().getPeerId().equals(n.getLeaderId())) {
                            leader = n;
                        }
                    }
                }
                assumeTrue("Could not transfer leadership away from node0", leader != node0);
            }

            assertNotNull("Should still have a leader", leader);
            final PeerId leaderPeer = leader.getLeaderId();
            final int leaderIdx = peers.indexOf(leaderPeer);

            // Find the observer (not node0, not leader)
            Node observer = null;
            for (int i = 0; i < nodes.size(); i++) {
                if (i != 0 && i != leaderIdx) {
                    observer = nodes.get(i);
                    break;
                }
            }
            assertNotNull("Should have an observer", observer);

            final long observerTermBefore = ((NodeImpl) observer).getCurrentTerm();

            // Enable fail flag: next setTermAndVotedFor on node0 returns false
            failFlag.set(true);

            // Directly trigger electSelf() on node0. This is synchronous:
            // electSelf() will increment term, call setTermAndVotedFor() which
            // fails, then stepDown and return — all without sending RequestVote
            // RPCs. No need to kill the leader and wait for timeout-driven elections.
            ((NodeImpl) node0).tryElectSelf();

            // failLatch was counted down synchronously inside setTermAndVotedFor
            assertTrue("Node0 should have attempted election and hit persist failure",
                failLatch.await(1, TimeUnit.SECONDS));

            final long observerTermAfter = ((NodeImpl) observer).getCurrentTerm();

            System.out.println("Observer term before=" + observerTermBefore + ", after=" + observerTermAfter
                               + ", node0 failed at term=" + failedAtTerm.get());

            // With the fix: setTermAndVotedFor is called BEFORE the RPC loop.
            // When it returns false, electSelf() steps down and returns without
            // sending any RequestVote RPCs. Therefore the observer's term should
            // NOT have been bumped by node0's failed election attempt.
            assertEquals("Observer term should not advance (no RequestVote RPC sent)", observerTermBefore,
                observerTermAfter);

            assertFalse("Node0 should not be leader after persist failure", ((NodeImpl) node0).isLeader());

        } finally {
            for (final RaftGroupService svc : services) {
                try {
                    svc.shutdown();
                    svc.join();
                } catch (final Exception ignored) {
                }
            }
        }
    }

    /**
     * Verify the same persist-before-RPC guarantee on the timeout-driven election path.
     * <p>
     * This keeps the "leader crash triggers a real election" setup, but uses a
     * counting RaftClientService on node0 to assert that no RequestVote RPC was
     * sent after setTermAndVotedFor() failed.
     */
    @Test
    public void testPersistFailurePreventsRpcSendAfterLeaderFailure() throws Exception {
        // Set NotElected priority on node2's PeerId in the Configuration itself,
        // so the leader's replicatorMap sees it and findTheNextCandidate() skips node2.
        // NodeOptions.setElectionPriority alone is NOT enough — it only sets the node's
        // own serverId.priority, which the leader doesn't see.
        final List<Integer> priorities = new ArrayList<>();
        priorities.add(ElectionPriority.Disabled); // node0: normal (Disabled means no priority election)
        priorities.add(ElectionPriority.Disabled); // node1: normal
        priorities.add(ElectionPriority.NotElected); // node2: never elected → leader skips in TimeoutNow
        final List<PeerId> peers = TestUtils.generatePriorityPeers(3, priorities);
        final String groupId = "electself-integration-test";
        final Configuration conf = new Configuration(peers);

        final AtomicBoolean failFlag = new AtomicBoolean(false);
        final AtomicLong failedAtTerm = new AtomicLong(-1);
        final CountDownLatch failLatch = new CountDownLatch(1);

        final List<RaftGroupService> services = new ArrayList<>();
        final List<Node> nodes = new ArrayList<>();

        for (int i = 0; i < peers.size(); i++) {
            final PeerId peer = peers.get(i);
            final NodeOptions nodeOptions = new NodeOptions();
            if (i == 1) {
                // node1: shortest timeout → always becomes leader first
                nodeOptions.setElectionTimeoutMs(300);
            } else {
                // node0 & node2: long timeout → never win initial election.
                // node0 gets triggered by TimeoutNow from dying leader, not by its own timer.
                nodeOptions.setElectionTimeoutMs(10_000);
            }
            nodeOptions.setSnapshotIntervalSecs(300);
            nodeOptions.setInitialConf(conf);

            final String serverDataPath = this.dataPath + File.separator + groupId + File.separator
                                          + peer.getEndpoint().toString().replace(':', '_');
            FileUtils.forceMkdir(new File(serverDataPath));
            nodeOptions.setLogUri(serverDataPath + File.separator + "logs");
            nodeOptions.setRaftMetaUri(serverDataPath + File.separator + "meta");
            nodeOptions.setSnapshotUri(serverDataPath + File.separator + "snapshot");
            nodeOptions.setFsm(new StateMachineAdapter() {
                @Override
                public void onApply(final Iterator iter) {
                    while (iter.hasNext()) {
                        iter.next();
                    }
                }
            });

            if (i == 0) {
                nodeOptions.setServiceFactory(new FailingServiceFactory(failFlag, failedAtTerm, failLatch));
            }

            final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(peer.getEndpoint());
            final RaftGroupService svc = new RaftGroupService(groupId, peer, nodeOptions, rpcServer);
            final Node node = svc.start();
            assertNotNull(node);
            services.add(svc);
            nodes.add(node);
        }

        try {
            Node leader = waitForLeader(nodes, 5000);
            assertNotNull("Should have a leader", leader);

            final Node node0 = nodes.get(0);
            if (leader == node0) {
                assertTrue("Leadership transfer from node0 should succeed", leader.transferLeadershipTo(peers.get(1))
                    .isOk());
                leader = waitForLeaderExcluding(nodes, node0, 5000);
                assumeTrue("Could not transfer leadership away from node0", leader != null && leader != node0);
            }

            final CountingRaftClientService countingRpc = new CountingRaftClientService(
                ((NodeImpl) node0).getRpcService());
            replaceRpcService((NodeImpl) node0, countingRpc);

            failFlag.set(true);

            final int leaderIdx = peers.indexOf(leader.getNodeId().getPeerId());
            services.get(leaderIdx).shutdown();
            services.get(leaderIdx).join();

            assertTrue("Node0 should have attempted timeout-driven election and hit persist failure",
                failLatch.await(8, TimeUnit.SECONDS));
            assertEquals("Node0 should not send RequestVote RPCs when persist fails", 0L,
                countingRpc.getRequestVoteCalls());
            assertEquals("Node0 should step down to follower after persist failure", State.STATE_FOLLOWER,
                ((NodeImpl) node0).getNodeState());
            assertTrue("Node0 should have recorded a failed election term", failedAtTerm.get() > 0);
        } finally {
            for (final RaftGroupService svc : services) {
                try {
                    svc.shutdown();
                    svc.join();
                } catch (final Exception ignored) {
                }
            }
        }
    }

    private static Node waitForLeader(final List<Node> nodes, final long timeoutMs) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            for (final Node node : nodes) {
                if (node.getNodeId().getPeerId().equals(node.getLeaderId())) {
                    return node;
                }
            }
            Thread.sleep(10);
        }
        return null;
    }

    private static Node waitForLeaderExcluding(final List<Node> nodes, final Node excluded, final long timeoutMs)
                                                                                                                 throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            for (final Node node : nodes) {
                if (node != excluded && node.getNodeId().getPeerId().equals(node.getLeaderId())) {
                    return node;
                }
            }
            Thread.sleep(10);
        }
        return null;
    }

    private static void replaceRpcService(final NodeImpl node, final RaftClientService rpcService) throws Exception {
        final Field field = NodeImpl.class.getDeclaredField("rpcService");
        field.setAccessible(true);
        field.set(node, rpcService);
    }

    /**
     * A RaftMetaStorage wrapper that returns false from setTermAndVotedFor()
     * when the fail flag is set, simulating a disk I/O error during election.
     */
    static class FailingMetaStorage implements RaftMetaStorage {
        private final LocalRaftMetaStorage delegate;
        private final AtomicBoolean        failFlag;
        private final AtomicLong           failedAtTerm;
        private final CountDownLatch       failLatch;

        FailingMetaStorage(String uri, RaftOptions raftOptions, AtomicBoolean failFlag, AtomicLong failedAtTerm,
                           CountDownLatch failLatch) {
            this.delegate = new LocalRaftMetaStorage(uri, raftOptions);
            this.failFlag = failFlag;
            this.failedAtTerm = failedAtTerm;
            this.failLatch = failLatch;
        }

        @Override
        public boolean init(final RaftMetaStorageOptions opts) {
            return this.delegate.init(opts);
        }

        @Override
        public void shutdown() {
            this.delegate.shutdown();
        }

        @Override
        public boolean setTerm(final long term) {
            return this.delegate.setTerm(term);
        }

        @Override
        public long getTerm() {
            return this.delegate.getTerm();
        }

        @Override
        public boolean setVotedFor(final PeerId peerId) {
            return this.delegate.setVotedFor(peerId);
        }

        @Override
        public PeerId getVotedFor() {
            return this.delegate.getVotedFor();
        }

        @Override
        public boolean setTermAndVotedFor(final long term, final PeerId peerId) {
            if (this.failFlag.get()) {
                this.failedAtTerm.set(term);
                this.failLatch.countDown();
                return false; // simulate disk I/O failure
            }
            return this.delegate.setTermAndVotedFor(term, peerId);
        }
    }

    static class FailingServiceFactory extends DefaultJRaftServiceFactory {
        private final AtomicBoolean  failFlag;
        private final AtomicLong     failedAtTerm;
        private final CountDownLatch failLatch;

        FailingServiceFactory(AtomicBoolean failFlag, AtomicLong failedAtTerm, CountDownLatch failLatch) {
            this.failFlag = failFlag;
            this.failedAtTerm = failedAtTerm;
            this.failLatch = failLatch;
        }

        @Override
        public RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions) {
            return new FailingMetaStorage(uri, raftOptions, this.failFlag, this.failedAtTerm, this.failLatch);
        }
    }

    static class CountingRaftClientService implements RaftClientService {
        private final RaftClientService delegate;
        private final AtomicLong        requestVoteCalls = new AtomicLong();

        CountingRaftClientService(final RaftClientService delegate) {
            this.delegate = delegate;
        }

        long getRequestVoteCalls() {
            return this.requestVoteCalls.get();
        }

        @Override
        public boolean init(final RpcOptions opts) {
            return this.delegate.init(opts);
        }

        @Override
        public void shutdown() {
            this.delegate.shutdown();
        }

        @Override
        public boolean connect(final Endpoint endpoint) {
            return this.delegate.connect(endpoint);
        }

        @Override
        public boolean checkConnection(final Endpoint endpoint, final boolean createIfAbsent) {
            return this.delegate.checkConnection(endpoint, createIfAbsent);
        }

        @Override
        public boolean disconnect(final Endpoint endpoint) {
            return this.delegate.disconnect(endpoint);
        }

        @Override
        public boolean isConnected(final Endpoint endpoint) {
            return this.delegate.isConnected(endpoint);
        }

        @Override
        public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                                  final RpcResponseClosure<T> done, final int timeoutMs) {
            return this.delegate.invokeWithDone(endpoint, request, done, timeoutMs);
        }

        @Override
        public Future<Message> preVote(final Endpoint endpoint, final RpcRequests.RequestVoteRequest request,
                                       final RpcResponseClosure<RpcRequests.RequestVoteResponse> done) {
            return this.delegate.preVote(endpoint, request, done);
        }

        @Override
        public Future<Message> requestVote(final Endpoint endpoint, final RpcRequests.RequestVoteRequest request,
                                           final RpcResponseClosure<RpcRequests.RequestVoteResponse> done) {
            this.requestVoteCalls.incrementAndGet();
            return this.delegate.requestVote(endpoint, request, done);
        }

        @Override
        public Future<Message> appendEntries(final Endpoint endpoint, final RpcRequests.AppendEntriesRequest request,
                                             final int timeoutMs,
                                             final RpcResponseClosure<RpcRequests.AppendEntriesResponse> done) {
            return this.delegate.appendEntries(endpoint, request, timeoutMs, done);
        }

        @Override
        public Future<Message> installSnapshot(final Endpoint endpoint,
                                               final RpcRequests.InstallSnapshotRequest request,
                                               final RpcResponseClosure<RpcRequests.InstallSnapshotResponse> done) {
            return this.delegate.installSnapshot(endpoint, request, done);
        }

        @Override
        public Future<Message> getFile(final Endpoint endpoint, final RpcRequests.GetFileRequest request,
                                       final int timeoutMs, final RpcResponseClosure<RpcRequests.GetFileResponse> done) {
            return this.delegate.getFile(endpoint, request, timeoutMs, done);
        }

        @Override
        public Future<Message> timeoutNow(final Endpoint endpoint, final RpcRequests.TimeoutNowRequest request,
                                          final int timeoutMs,
                                          final RpcResponseClosure<RpcRequests.TimeoutNowResponse> done) {
            return this.delegate.timeoutNow(endpoint, request, timeoutMs, done);
        }

        @Override
        public Future<Message> readIndex(final Endpoint endpoint, final RpcRequests.ReadIndexRequest request,
                                         final int timeoutMs,
                                         final RpcResponseClosure<RpcRequests.ReadIndexResponse> done) {
            return this.delegate.readIndex(endpoint, request, timeoutMs, done);
        }
    }
}
