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
package com.alipay.sofa.jraft.entity;

import java.util.List;

import com.alipay.sofa.jraft.Quorum;
import com.alipay.sofa.jraft.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.test.TestUtils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BallotTest {

    private Ballot ballot;

    @Before
    public void setup() {
        this.ballot = new Ballot();
        Configuration configuration = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083");
        Quorum quorum = BallotFactory.buildMajorityQuorum(configuration.getPeers().size());
        configuration.setQuorum(quorum);
        this.ballot.init(configuration, null);
    }

    @Test
    public void testGrant() {
        PeerId peer1 = new PeerId("localhost", 8081);
        this.ballot.grant(peer1);
        assertFalse(this.ballot.isGranted());

        PeerId unfoundPeer = new PeerId("localhost", 8084);
        this.ballot.grant(unfoundPeer);
        assertFalse(this.ballot.isGranted());

        PeerId peer2 = new PeerId("localhost", 8082);
        this.ballot.grant(peer2);
        assertTrue(this.ballot.isGranted());
    }

    @Test
    public void testElectionRequiresThreeVotesWhenWriteQuorumIsTwo() {
        final Configuration conf = flexibleConfiguration(4, 4);
        final List<PeerId> peers = conf.getPeers();
        final Ballot election = Ballot.forElection();
        election.init(conf, null);
        election.grant(peers.get(0));
        election.grant(peers.get(1));
        final boolean grantedWithTwoVotes = election.isGranted();
        assertFalse(grantedWithTwoVotes);
        election.grant(peers.get(2));
        final boolean grantedWithThreeVotes = election.isGranted();
        assertTrue(grantedWithThreeVotes);
    }

    @Test
    public void testElectionIntersectsSingleCopyWrites() {
        final Configuration conf = flexibleConfiguration(5, 2);
        final List<PeerId> peers = conf.getPeers();
        final Ballot election = Ballot.forElection();
        election.init(conf, null);
        for (int i = 0; i < peers.size() - 1; i++) {
            election.grant(peers.get(i));
            final boolean granted = election.isGranted();
            assertFalse(granted);
        }
        election.grant(peers.get(4));
        final boolean granted = election.isGranted();
        assertTrue(granted);
    }

    @Test
    public void testElectionRequiresMajorityWhenReadQuorumIsOne() {
        final Configuration conf = flexibleConfiguration(4, 8);
        final List<PeerId> peers = conf.getPeers();
        final Ballot election = Ballot.forElection();
        election.init(conf, null);
        election.grant(peers.get(0));
        election.grant(peers.get(1));
        final boolean grantedWithTwoVotes = election.isGranted();
        assertFalse(grantedWithTwoVotes);
        election.grant(peers.get(2));
        final boolean grantedWithThreeVotes = election.isGranted();
        assertTrue(grantedWithThreeVotes);
    }

    @Test
    public void testElectionRequiresBothConfigurations() {
        final Configuration conf = flexibleConfiguration(4, 4);
        final Configuration oldConf = flexibleConfiguration(5, 2);
        final List<PeerId> peers = oldConf.getPeers();
        final Ballot election = Ballot.forElection();
        election.init(conf, oldConf);
        for (int i = 0; i < peers.size() - 1; i++) {
            election.grant(peers.get(i));
            final boolean granted = election.isGranted();
            assertFalse(granted);
        }
        election.grant(peers.get(4));
        final boolean granted = election.isGranted();
        assertTrue(granted);
    }

    @Test
    public void testRefreshPreservesElectionQuorum() {
        final Configuration conf = flexibleConfiguration(4, 8);
        final Ballot election = Ballot.forElection();
        election.init(conf, null);
        final Configuration updatedConf = flexibleConfiguration(4, 4);
        election.refreshBallot(updatedConf, null);
        final List<PeerId> peers = updatedConf.getPeers();
        election.grant(peers.get(0));
        election.grant(peers.get(1));
        final boolean grantedWithTwoVotes = election.isGranted();
        assertFalse(grantedWithTwoVotes);
        election.grant(peers.get(2));
        final boolean grantedWithThreeVotes = election.isGranted();
        assertTrue(grantedWithThreeVotes);
    }

    @Test
    public void testWriteBallotStillUsesWriteQuorum() {
        final Configuration conf = flexibleConfiguration(4, 4);
        final List<PeerId> peers = conf.getPeers();
        this.ballot.init(conf, null);
        this.ballot.grant(peers.get(0));
        final boolean grantedWithOneVote = this.ballot.isGranted();
        assertFalse(grantedWithOneVote);
        this.ballot.grant(peers.get(1));
        final boolean grantedWithTwoVotes = this.ballot.isGranted();
        assertTrue(grantedWithTwoVotes);
    }

    private Configuration flexibleConfiguration(final int size, final int writeFactor) {
        final List<PeerId> peers = TestUtils.generatePeers(size);
        final Configuration conf = new Configuration(peers);
        final int readFactor = 10 - writeFactor;
        final Quorum quorum = BallotFactory.buildFlexibleQuorum(readFactor, writeFactor, size);
        conf.setQuorum(quorum);
        conf.setEnableFlexible(true);
        return conf;
    }
}
