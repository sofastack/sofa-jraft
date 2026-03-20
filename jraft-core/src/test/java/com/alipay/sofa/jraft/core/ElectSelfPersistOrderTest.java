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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.impl.LocalRaftMetaStorage;
import com.alipay.sofa.jraft.test.TestUtils;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Test that electSelf() persists (term, votedFor) BEFORE sending RequestVote RPCs.
 *
 * Before the fix, electSelf() sent RequestVote RPCs to all peers in a loop and
 * only then called metaStorage.setTermAndVotedFor(). If the node crashed between
 * sending the RPCs and persisting, on restart it would not remember its own vote,
 * allowing it to vote for a different candidate in the same term — a Raft safety
 * violation (Section 5.2 of the Raft paper).
 *
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
     *
     * Approach:
     * 1. Start a 3-node cluster with a FailingMetaStorage on node 0
     * 2. Wait for leader election to complete normally
     * 3. Enable the failure flag on node 0's meta storage
     * 4. Stop the current leader to force new elections
     * 5. Node 0 will attempt electSelf(), fail on setTermAndVotedFor(), step down
     * 6. Verify the observer node's term did NOT advance due to node 0's failed election
     *    (proving no RequestVote RPC was sent before the persistence check)
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
            // node0 gets a short election timeout so it fires first after leader death.
            // Other nodes get a long timeout so they won't independently start elections
            // during the test window — this lets us assert term equality on the observer.
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
            int observerIdx = -1;
            for (int i = 0; i < nodes.size(); i++) {
                if (i != 0 && i != leaderIdx) {
                    observer = nodes.get(i);
                    observerIdx = i;
                    break;
                }
            }
            assertNotNull("Should have an observer", observer);

            final long observerTermBefore = ((NodeImpl) observer).getCurrentTerm();

            // Enable fail flag: next setTermAndVotedFor on node0 returns false
            failFlag.set(true);

            // Stop leader to trigger elections
            services.get(leaderIdx).shutdown();
            services.get(leaderIdx).join();

            // Wait for node0 to attempt election and hit the persist failure
            boolean failed = failLatch.await(10, TimeUnit.SECONDS);
            assertTrue("Node0 should have attempted election", failed);

            final long failTerm = failedAtTerm.get();

            // Brief pause for any in-flight messages
            Thread.sleep(500);

            final long observerTermAfter = ((NodeImpl) observer).getCurrentTerm();

            // With the fix: setTermAndVotedFor is called BEFORE the RPC loop.
            // When it returns false, electSelf() steps down and returns without
            // sending any RequestVote RPCs. Therefore the observer's term should
            // NOT have been bumped by node0's failed election attempt.
            //
            // Without the fix: RPCs were sent BEFORE setTermAndVotedFor, so the
            // observer would have already received the RequestVote and bumped its
            // term even though node0's persistence failed.
            //
            // The observer uses a long election timeout (10s) so it won't
            // independently start its own election during this short test window.
            System.out.println("Observer term before=" + observerTermBefore + ", after=" + observerTermAfter
                               + ", node0 failed at term=" + failTerm);

            // Observer's term must not change — proves no RequestVote RPC was sent.
            assertEquals("Observer term should not advance (no RequestVote RPC sent)", observerTermBefore,
                observerTermAfter);

            // With the fix, node0's persist failure means it steps down and
            // does NOT send RequestVote RPCs. Verify node0 is no longer leader.
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
}
