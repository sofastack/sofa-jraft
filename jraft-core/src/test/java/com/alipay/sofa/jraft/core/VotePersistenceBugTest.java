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
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteResponse;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.impl.LocalRaftMetaStorage;
import com.alipay.sofa.jraft.test.TestUtils;

import static org.junit.Assert.*;

/**
 * Test that handleRequestVoteRequest checks the return value of setVotedFor().
 *
 * Before the fix, handleRequestVoteRequest() set the in-memory votedId before
 * calling metaStorage.setVotedFor() and never checked the return value.  If the
 * disk write failed (I/O error) but the process survived, the response was
 * still granted=true because the in-memory votedId had already been set.  After
 * a restart the vote was lost, allowing a different candidate to be granted the
 * vote in the same term — a Raft safety violation.
 *
 * @see <a href="https://github.com/sofastack/sofa-jraft/issues/1241">#1241</a>
 */
public class VotePersistenceBugTest {

    private String dataPath;

    @Before
    public void setup() throws Exception {
        this.dataPath = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.dataPath));
        assertEquals(NodeImpl.GLOBAL_NUM_NODES.get(), 0);
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
        try {
            FileUtils.deleteDirectory(new File(this.dataPath));
        } finally {
            NodeManager.getInstance().clear();
        }
    }

    // Wraps LocalRaftMetaStorage.  When failNextSetVotedFor is true,
    // setVotedFor() returns false to simulate a disk I/O error.
    static class DiskFailureMetaStorage implements RaftMetaStorage {
        private final LocalRaftMetaStorage delegate;
        volatile boolean                   failNextSetVotedFor = false;

        DiskFailureMetaStorage(final String uri, final RaftOptions raftOptions) {
            this.delegate = new LocalRaftMetaStorage(uri, raftOptions);
        }

        @Override
        public boolean init(final RaftMetaStorageOptions opts) {
            return delegate.init(opts);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public boolean setTerm(final long term) {
            return delegate.setTerm(term);
        }

        @Override
        public long getTerm() {
            return delegate.getTerm();
        }

        @Override
        public PeerId getVotedFor() {
            return delegate.getVotedFor();
        }

        @Override
        public boolean setTermAndVotedFor(final long term, final PeerId peerId) {
            return delegate.setTermAndVotedFor(term, peerId);
        }

        @Override
        public boolean setVotedFor(final PeerId peerId) {
            if (failNextSetVotedFor) {
                failNextSetVotedFor = false;
                return false;
            }
            return delegate.setVotedFor(peerId);
        }
    }

    static class DiskFailureServiceFactory extends TestJRaftServiceFactory {
        volatile DiskFailureMetaStorage storage;

        @Override
        public RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions) {
            storage = new DiskFailureMetaStorage(uri, raftOptions);
            return storage;
        }
    }

    private RequestVoteRequest buildVoteRequest(final PeerId candidate, final PeerId voter, final long term) {
        return RequestVoteRequest.newBuilder().setGroupId("unittest").setServerId(candidate.toString())
            .setPeerId(voter.toString()).setTerm(term).setLastLogIndex(999_999).setLastLogTerm(999_999)
            .setPreVote(false).build();
    }

    /**
     * Control: normal restart preserves vote, second candidate rejected.
     */
    @Test
    public void testVoteRejectedAfterNormalRestart() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);
        final PeerId voter = peers.get(0);
        final PeerId candidate1 = peers.get(1);
        final PeerId candidate2 = peers.get(2);

        final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers, new LinkedHashSet<>(), 600_000);
        assertTrue(cluster.start(voter.getEndpoint()));
        Thread.sleep(1000);

        final NodeImpl node1 = (NodeImpl) cluster.getNodes().get(0);
        final RequestVoteResponse resp1 = (RequestVoteResponse) node1.handleRequestVoteRequest(buildVoteRequest(
            candidate1, voter, 100));
        assertTrue("first vote should be granted", resp1.getGranted());

        cluster.stop(voter.getEndpoint());
        Thread.sleep(200);
        assertTrue(cluster.start(voter.getEndpoint()));
        Thread.sleep(1000);

        // After normal restart, vote for candidate2 in the same term should be rejected
        final NodeImpl node2 = (NodeImpl) cluster.getNodes().get(0);
        final RequestVoteResponse resp2 = (RequestVoteResponse) node2.handleRequestVoteRequest(buildVoteRequest(
            candidate2, voter, 100));
        assertFalse("vote should be rejected after restart — already voted for candidate1", resp2.getGranted());

        cluster.stopAll();
    }

    /**
     * setVotedFor() I/O failure: the real LocalRaftMetaStorage.save() fails
     * because the meta directory is made read-only.
     *
     * With the fix, handleRequestVoteRequest() checks the return value of
     * setVotedFor() and rolls back the in-memory votedId on failure, so the
     * response is granted=false.
     */
    @Test
    public void testVoteNotGrantedWhenSetVotedForFails() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);
        final PeerId voter = peers.get(0);
        final PeerId candidate1 = peers.get(1);

        final DiskFailureServiceFactory factory = new DiskFailureServiceFactory();
        final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers, new LinkedHashSet<>(), 600_000);
        cluster.setRaftServiceFactory(factory);
        assertTrue(cluster.start(voter.getEndpoint()));
        Thread.sleep(1000);

        // Enable I/O failure for the next setVotedFor() call
        factory.storage.failNextSetVotedFor = true;

        // Vote for candidate1; setVotedFor() will hit a real I/O error.
        // With the fix, the vote must NOT be granted.
        final NodeImpl node1 = (NodeImpl) cluster.getNodes().get(0);
        final RequestVoteResponse resp1 = (RequestVoteResponse) node1.handleRequestVoteRequest(buildVoteRequest(
            candidate1, voter, 100));
        assertFalse("vote must not be granted when setVotedFor() I/O fails", resp1.getGranted());

        cluster.stopAll();
    }
}
