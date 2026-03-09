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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

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
 * Test for unchecked setVotedFor() return value in handleRequestVoteRequest().
 *
 * handleRequestVoteRequest() does not check the return value of
 * metaStorage.setVotedFor(). If the disk write fails (I/O error) but the
 * process survives, the response is still granted=true because the in-memory
 * votedId was already set before the disk write. After restart the vote is
 * lost, allowing a different candidate to be granted in the same term.
 */
public class VotePersistenceBugTest {

    @Rule
    public TestName testName = new TestName();

    private String  dataPath;

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
                c.stopAll();
            }
        }
        FileUtils.deleteDirectory(new File(this.dataPath));
        NodeManager.getInstance().clear();
    }

    // Wraps LocalRaftMetaStorage. When failNextSetVotedFor is true, makes the
    // meta directory read-only before delegating setVotedFor() so that the real
    // save() fails with an I/O error, triggering the full reportIOError() ->
    // node.onError() path.
    static class DiskFailureMetaStorage implements RaftMetaStorage {
        private final LocalRaftMetaStorage delegate;
        private final String               metaDir;
        volatile boolean                   failNextSetVotedFor = false;

        DiskFailureMetaStorage(final String uri, final RaftOptions raftOptions) {
            this.delegate = new LocalRaftMetaStorage(uri, raftOptions);
            this.metaDir = uri;
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
                final File dir = new File(metaDir);
                dir.setWritable(false);
                try {
                    return delegate.setVotedFor(peerId);
                } finally {
                    dir.setWritable(true);
                }
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

    // Control: normal restart preserves vote, second candidate rejected.
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
        assertTrue(resp1.getGranted());

        cluster.stop(voter.getEndpoint());
        Thread.sleep(200);
        assertTrue(cluster.start(voter.getEndpoint()));
        Thread.sleep(1000);

        // after normal restart, vote for candidate2 in the same term should be rejected
        final NodeImpl node2 = (NodeImpl) cluster.getNodes().get(0);
        final RequestVoteResponse resp2 = (RequestVoteResponse) node2.handleRequestVoteRequest(buildVoteRequest(
            candidate2, voter, 100));
        assertFalse(resp2.getGranted());

        cluster.stopAll();
    }

    // setVotedFor() I/O failure: the real LocalRaftMetaStorage.save() fails
    // because the meta directory is made read-only. This triggers the full
    // reportIOError() -> node.onError() -> STATE_ERROR path. Despite this,
    // handleRequestVoteRequest() still returns granted=true because the
    // return value of setVotedFor() is not checked.
    @Test
    public void testDoubleVoteAfterSetVotedForIOFailure() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);
        final PeerId voter = peers.get(0);
        final PeerId candidate1 = peers.get(1);
        final PeerId candidate2 = peers.get(2);

        final DiskFailureServiceFactory factory = new DiskFailureServiceFactory();
        final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers, new LinkedHashSet<>(), 600_000);
        cluster.setRaftServiceFactory(factory);
        assertTrue(cluster.start(voter.getEndpoint()));
        Thread.sleep(1000);

        // enable I/O failure for the next setVotedFor() call
        factory.storage.failNextSetVotedFor = true;

        // vote for candidate1; setVotedFor() will hit a real I/O error
        final NodeImpl node1 = (NodeImpl) cluster.getNodes().get(0);
        final RequestVoteResponse resp1 = (RequestVoteResponse) node1.handleRequestVoteRequest(buildVoteRequest(
            candidate1, voter, 100));
        // response is granted=true despite the disk write failure
        assertTrue(resp1.getGranted());

        // stop and restart with normal storage
        cluster.stop(voter.getEndpoint());
        Thread.sleep(200);
        cluster.setRaftServiceFactory(new TestJRaftServiceFactory());
        assertTrue(cluster.start(voter.getEndpoint()));
        Thread.sleep(1000);

        // vote for candidate2 in the same term — should be rejected
        final NodeImpl node2 = (NodeImpl) cluster.getNodes().get(0);
        final RequestVoteResponse resp2 = (RequestVoteResponse) node2.handleRequestVoteRequest(buildVoteRequest(
            candidate2, voter, 100));
        assertFalse("double-vote: node voted for both " + candidate1 + " and " + candidate2 + " in term 100",
            resp2.getGranted());

        cluster.stopAll();
    }
}
