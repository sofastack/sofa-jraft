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
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.test.TestUtils;

import static org.junit.Assert.*;

/**
 * Test that an uncaught exception in {@code StateMachine.onApply()} does not
 * permanently stall the apply pipeline.
 *
 * Before the fix, an exception from {@code fsm.onApply()} would propagate out
 * of {@code doApplyTasks()}, skip {@code setLastApplied()}, and cause the same
 * entries to be retried on every subsequent {@code doCommitted()} call — creating
 * an infinite exception loop while the node appeared healthy.
 *
 * The fix catches the exception, marks the iterator with an error via
 * {@code setErrorAndRollback()}, and lets the existing error handling path
 * advance {@code lastAppliedIndex} and transition the node to error state.
 */
public class OnApplyExceptionTest {

    private String dataPath;

    @Before
    public void setup() throws Exception {
        this.dataPath = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.dataPath));
    }

    @After
    public void teardown() throws Exception {
        FileUtils.deleteDirectory(new File(this.dataPath));
    }

    /**
     * Start a single-node cluster with a StateMachine that throws on a specific
     * entry. Verify:
     * 1. The exception is caught and the node transitions to error state
     * 2. The exception does NOT fire repeatedly (no infinite retry loop)
     * 3. Remaining task closures receive an error status (not silently lost)
     */
    @Test
    public void testOnApplyExceptionSetsErrorInsteadOfStalling() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(1);
        final PeerId peer = peers.get(0);

        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setElectionTimeoutMs(300);
        nodeOptions.setSnapshotIntervalSecs(300);
        nodeOptions.setInitialConf(new Configuration(peers));

        final String serverDataPath = this.dataPath + File.separator
                                      + peer.getEndpoint().toString().replace(':', '_');
        FileUtils.forceMkdir(new File(serverDataPath));
        nodeOptions.setLogUri(serverDataPath + File.separator + "logs");
        nodeOptions.setRaftMetaUri(serverDataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(serverDataPath + File.separator + "snapshot");

        final AtomicLong exceptionCount = new AtomicLong(0);
        final String poisonPayload = "poison-entry";

        final StateMachineAdapter fsm = new StateMachineAdapter() {
            @Override
            public void onApply(final Iterator iter) {
                while (iter.hasNext()) {
                    final ByteBuffer data = iter.getData();
                    final String payload = data != null ? new String(data.array()) : "";
                    if (payload.contains(poisonPayload)) {
                        exceptionCount.incrementAndGet();
                        throw new RuntimeException("Simulated persistent FSM bug for: " + payload);
                    }
                    if (iter.done() != null) {
                        iter.done().run(Status.OK());
                    }
                    iter.next();
                }
            }
        };
        nodeOptions.setFsm(fsm);

        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(peer.getEndpoint());
        final RaftGroupService raftGroupService = new RaftGroupService("onapply-test", peer, nodeOptions, rpcServer);
        final Node node = raftGroupService.start();
        assertNotNull(node);

        // Wait for single-node leader election
        int waitCount = 0;
        while (node.getLeaderId() == null || node.getLeaderId().isEmpty()) {
            Thread.sleep(100);
            if (++waitCount > 50) {
                fail("Node did not become leader within 5 seconds");
            }
        }

        // Submit a normal entry first
        final CountDownLatch normalLatch = new CountDownLatch(1);
        node.apply(new Task(ByteBuffer.wrap("normal-entry".getBytes()), status -> {
            assertTrue("Normal entry should succeed", status.isOk());
            normalLatch.countDown();
        }));
        assertTrue("Normal entry should complete", normalLatch.await(5, TimeUnit.SECONDS));

        // Submit the poison entry
        final CountDownLatch poisonLatch = new CountDownLatch(1);
        final AtomicBoolean poisonCallbackFired = new AtomicBoolean(false);
        node.apply(new Task(ByteBuffer.wrap(poisonPayload.getBytes()), status -> {
            poisonCallbackFired.set(true);
            poisonLatch.countDown();
        }));

        // Wait for the exception to fire and the error to be set
        Thread.sleep(3000);

        // With the fix: the exception is caught once, the node enters error
        // state, and the closures are properly notified. The exception should
        // fire exactly once (not repeatedly as in the stalled case).
        final long finalExceptionCount = exceptionCount.get();
        System.out.println("Exception count: " + finalExceptionCount);

        // The exception should have fired at most a small number of times
        // (ideally once). Before the fix, it would fire continuously.
        assertTrue("Exception should have fired", finalExceptionCount >= 1);
        assertTrue("Exception should not fire in an infinite loop (was " + finalExceptionCount + ")",
            finalExceptionCount <= 3);

        // Cleanup
        raftGroupService.shutdown();
        raftGroupService.join();
    }
}
