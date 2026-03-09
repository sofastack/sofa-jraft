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
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.test.TestUtils;

import static org.junit.Assert.*;

/**
 * Integration test that exercises a 3-node Raft cluster with tracing enabled.
 * Produces an NDJSON trace file suitable for TLA+ trace validation via Tracesofajraft.tla.
 *
 * Run with:
 *   mvn test -pl jraft-core -Dtest=RaftTraceTest -Djraft.trace.enabled=true \
 *       -Djraft.trace.file=/tmp/sofa-jraft-trace.ndjson
 */
public class RaftTraceTest {

    private String      dataPath;
    private TestCluster cluster;

    @Before
    public void setup() throws Exception {
        this.dataPath = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.dataPath));
    }

    @After
    public void teardown() throws Exception {
        if (this.cluster != null) {
            this.cluster.stopAll();
        }
        if (!TestCluster.CLUSTERS.isEmpty()) {
            for (final TestCluster c : TestCluster.CLUSTERS.removeAll()) {
                c.stopAll();
            }
        }
        RaftTracer.shutdown();
        FileUtils.deleteDirectory(new File(this.dataPath));
    }

    @Test
    public void testThreeNodeClusterWithTrace() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);

        // Register simplified ID mappings for trace: s1, s2, s3
        for (int i = 0; i < peers.size(); i++) {
            RaftTracer.registerMapping(peers.get(i), "s" + (i + 1));
        }

        this.cluster = new TestCluster("unittest", this.dataPath, peers);

        // Start all 3 nodes
        for (final PeerId peer : peers) {
            assertTrue("Failed to start node " + peer, this.cluster.start(peer.getEndpoint()));
        }

        // Wait for leader election — exercises:
        //   BecomeCandidate, HandleRequestVoteRequest, HandleRequestVoteResponse, BecomeLeader
        this.cluster.waitLeader();
        final Node leader = this.cluster.getLeader();
        assertNotNull("Leader should be elected", leader);

        // Send client requests immediately — exercises:
        //   SendAppendEntries, HandleAppendEntriesRequest (replicate),
        //   HandleAppendEntriesResponseSuccess, AdvanceCommitIndex,
        //   SendHeartbeat, HandleHeartbeatResponse (interleaved)
        final int taskCount = 5;
        final CountDownLatch latch = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            final ByteBuffer data = ByteBuffer.wrap(("trace-test-" + i).getBytes());
            final Task task = new Task(data, new ExpectClosure(RaftError.SUCCESS, latch));
            leader.apply(task);
        }
        latch.await();

        // Verify replication
        final long expectedLastIndex = 1 + taskCount; // 1 (config) + taskCount
        assertTrue("Leader commitIndex should advance", leader.getLastCommittedIndex() >= expectedLastIndex);

        // Shutdown tracer immediately to stop accumulating heartbeat events
        RaftTracer.shutdown();

        System.out.println("=== Trace test completed ===");
        System.out.println("Leader: " + RaftTracer.mapId(leader.getNodeId().getPeerId()));
        System.out.println("CommitIndex: " + leader.getLastCommittedIndex());
        System.out.println("Trace enabled: " + RaftTracer.isEnabled());
        if (RaftTracer.isEnabled()) {
            System.out.println("Trace file: " + System.getProperty("jraft.trace.file", "trace.ndjson"));
        }
    }
}
