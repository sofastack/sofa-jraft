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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.closure.SynchronizedClosure;
import com.alipay.sofa.jraft.closure.TaskClosure;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.EventBusMode;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for different EventBus modes (DISRUPTOR and MPSC).
 * <p>
 * This test class runs the same test cases with both EventBus implementations
 * to ensure they behave identically.
 *
 * @author dennis
 * @see <a href="https://github.com/sofastack/sofa-jraft/issues/1231">Issue #1231</a>
 */
@RunWith(Parameterized.class)
public class NodeEventBusModeTest {

    private static final Logger LOG = LoggerFactory.getLogger(NodeEventBusModeTest.class);

    @Parameterized.Parameters(name = "EventBusMode={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { EventBusMode.DISRUPTOR }, { EventBusMode.MPSC } });
    }

    @Parameterized.Parameter
    public EventBusMode eventBusMode;

    private String      dataPath;

    @Rule
    public TestName     testName = new TestName();

    private long        testStartMs;

    @Before
    public void setup() throws Exception {
        LOG.info(">>>>>>>>>>>>>>> Start test: {} with EventBusMode={}", this.testName.getMethodName(),
            this.eventBusMode);
        this.dataPath = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.dataPath));
        assertEquals(NodeImpl.GLOBAL_NUM_NODES.get(), 0);
        this.testStartMs = Utils.monotonicMs();
    }

    @After
    public void teardown() throws Exception {
        if (!TestCluster.CLUSTERS.isEmpty()) {
            for (final TestCluster c : TestCluster.CLUSTERS.removeAll()) {
                c.stopAll();
            }
        }
        if (NodeImpl.GLOBAL_NUM_NODES.get() > 0) {
            Thread.sleep(5000);
            assertEquals(0, NodeImpl.GLOBAL_NUM_NODES.get());
        }
        FileUtils.deleteDirectory(new File(this.dataPath));
        LOG.info(">>>>>>>>>>>>>>> End test: {} with EventBusMode={}, cost: {} ms", this.testName.getMethodName(),
            this.eventBusMode, Utils.monotonicMs() - this.testStartMs);
    }

    private RaftOptions createRaftOptions() {
        final RaftOptions raftOptions = new RaftOptions();
        raftOptions.setEventBusMode(this.eventBusMode);
        return raftOptions;
    }

    /**
     * Test basic three-node cluster operations with apply and TaskClosure.
     */
    @Test
    public void testTripleNodesApply() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);
        final RaftOptions raftOptions = createRaftOptions();

        final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 300, false, null, raftOptions));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());

        // apply tasks to leader
        sendTestTaskAndWait(leader);

        assertEquals(11, leader.getLastCommittedIndex());
        assertEquals(11, leader.getLastLogIndex());
        Thread.sleep(500);
        assertEquals(11, leader.getLastAppliedLogIndex());

        // Test task with TaskClosure
        {
            final ByteBuffer data = ByteBuffer.wrap("task closure".getBytes());
            final Vector<String> cbs = new Vector<>();
            final CountDownLatch latch = new CountDownLatch(1);
            final Task task = new Task(data, new TaskClosure() {
                @Override
                public void run(final Status status) {
                    cbs.add("apply");
                    latch.countDown();
                }

                @Override
                public void onCommitted() {
                    cbs.add("commit");
                }
            });
            leader.apply(task);
            latch.await();
            assertEquals(2, cbs.size());
            assertEquals("commit", cbs.get(0));
            assertEquals("apply", cbs.get(1));
        }

        cluster.ensureSame(-1);
        cluster.stopAll();
    }

    /**
     * Test ReadIndex operations under chaos conditions with concurrent reads and writes.
     */
    @Test
    public void testReadIndexChaos() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);
        final RaftOptions raftOptions = createRaftOptions();

        final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 300, true, null, raftOptions));
        }

        // elect leader
        cluster.waitLeader();

        // get leader
        final Node leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(3, leader.listPeers().size());

        final int threads = 5;
        final int opsPerThread = 50;
        final CountDownLatch latch = new CountDownLatch(threads);
        final AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < threads; t++) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            try {
                                sendTestTaskAndWait(leader);
                            } catch (final InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            readIndexRandom(cluster);
                            successCount.incrementAndGet();
                        }
                    } finally {
                        latch.countDown();
                    }
                }

                private void readIndexRandom(final TestCluster cluster) {
                    final CountDownLatch readLatch = new CountDownLatch(1);
                    final byte[] requestContext = TestUtils.getRandomBytes();
                    cluster.getNodes().get(ThreadLocalRandom.current().nextInt(3))
                        .readIndex(requestContext, new ReadIndexClosure() {
                            @Override
                            public void run(final Status status, final long index, final byte[] reqCtx) {
                                if (status.isOk()) {
                                    assertTrue(index > 0);
                                    assertArrayEquals(requestContext, reqCtx);
                                }
                                readLatch.countDown();
                            }
                        });
                    try {
                        readLatch.await();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }.start();
        }

        latch.await();

        cluster.ensureSame();

        final int expectedLogs = threads * opsPerThread * 10; // 10 logs per sendTestTaskAndWait
        for (final MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(expectedLogs, fsm.getLogs().size());
        }

        LOG.info("testReadIndexChaos completed with {} successful operations", successCount.get());
        cluster.stopAll();
    }

    /**
     * Test leader failure and re-election scenario.
     */
    @Test
    public void testLeaderFailAndReElect() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);
        final RaftOptions raftOptions = createRaftOptions();

        final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 300, false, null, raftOptions));
        }

        // elect leader
        cluster.waitLeader();
        Node leader = cluster.getLeader();
        assertNotNull(leader);

        // apply some tasks
        sendTestTaskAndWait(leader);

        // stop leader
        final Endpoint leaderAddr = leader.getLeaderId().getEndpoint().copy();
        assertTrue(cluster.stop(leaderAddr));

        // wait for new leader
        cluster.waitLeader();
        leader = cluster.getLeader();
        assertNotNull(leader);

        // apply more tasks to new leader
        sendTestTaskAndWait(leader);

        // restart old leader
        assertTrue(cluster.start(leaderAddr, false, 300, false, null, raftOptions));

        cluster.ensureSame();
        cluster.stopAll();
    }

    /**
     * Test concurrent apply tasks from multiple threads.
     */
    @Test
    public void testConcurrentApply() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(3);
        final RaftOptions raftOptions = createRaftOptions();

        final TestCluster cluster = new TestCluster("unittest", this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint(), false, 300, false, null, raftOptions));
        }

        cluster.waitLeader();
        final Node leader = cluster.getLeader();
        assertNotNull(leader);

        final int threads = 10;
        final int tasksPerThread = 100;
        final CountDownLatch latch = new CountDownLatch(threads);
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger failCount = new AtomicInteger(0);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    for (int i = 0; i < tasksPerThread; i++) {
                        final ByteBuffer data = ByteBuffer.wrap(("thread-" + threadId + "-task-" + i).getBytes());
                        final SynchronizedClosure done = new SynchronizedClosure();
                        leader.apply(new Task(data, done));
                        final Status status = done.await();
                        if (status.isOk()) {
                            successCount.incrementAndGet();
                        } else {
                            failCount.incrementAndGet();
                        }
                    }
                } catch (final Exception e) {
                    LOG.error("Error in thread {}", threadId, e);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();

        LOG.info("Concurrent apply completed: success={}, fail={}", successCount.get(), failCount.get());
        assertEquals(threads * tasksPerThread, successCount.get());
        assertEquals(0, failCount.get());

        cluster.ensureSame();
        cluster.stopAll();
    }

    private void sendTestTaskAndWait(final Node node) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            final ByteBuffer data = ByteBuffer.wrap(("hello" + i).getBytes());
            final Task task = new Task(data, new SynchronizedClosure() {
                @Override
                public void run(final Status status) {
                    assertTrue(status.isOk());
                    latch.countDown();
                }
            });
            node.apply(task);
        }
        latch.await();
    }
}
