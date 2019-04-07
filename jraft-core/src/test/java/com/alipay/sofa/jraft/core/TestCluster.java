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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * Test cluster for NodeTest
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-20 1:41:17 PM
 */
public class TestCluster {
    private final String                                  dataPath;
    private final String                                  name;                                 // groupId
    private final List<PeerId>                            peers;
    private final List<NodeImpl>                          nodes;
    private final List<MockStateMachine>                  fsms;
    private final ConcurrentMap<String, RaftGroupService> serverMap = new ConcurrentHashMap<>();
    private final int                                     electionTimeoutMs;
    private final Lock                                    lock      = new ReentrantLock();

    public List<PeerId> getPeers() {
        return this.peers;
    }

    public TestCluster(String name, String dataPath, List<PeerId> peers) {
        this(name, dataPath, peers, 300);
    }

    public TestCluster(String name, String dataPath, List<PeerId> peers, int electionTimeoutMs) {
        super();
        this.name = name;
        this.dataPath = dataPath;
        this.peers = peers;
        this.nodes = new ArrayList<>(this.peers.size());
        this.fsms = new ArrayList<>(this.peers.size());
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public boolean start(Endpoint addr) throws Exception {
        return this.start(addr, false, 300);
    }

    public boolean start(Endpoint listenAddr, boolean emptyPeers, int snapshotIntervalSecs) throws IOException {
        return this.start(listenAddr, emptyPeers, snapshotIntervalSecs, false);
    }

    public boolean start(Endpoint listenAddr, boolean emptyPeers, int snapshotIntervalSecs, boolean enableMetrics)
                                                                                                                  throws IOException {
        return this.start(listenAddr, emptyPeers, snapshotIntervalSecs, enableMetrics, null, null);
    }

    public boolean start(Endpoint listenAddr, boolean emptyPeers, int snapshotIntervalSecs, boolean enableMetrics,
                         SnapshotThrottle snapshotThrottle) throws IOException {
        return this.start(listenAddr, emptyPeers, snapshotIntervalSecs, enableMetrics, snapshotThrottle, null);
    }

    public boolean start(Endpoint listenAddr, boolean emptyPeers, int snapshotIntervalSecs, boolean enableMetrics,
                         SnapshotThrottle snapshotThrottle, RaftOptions raftOptions) throws IOException {

        if (this.serverMap.get(listenAddr.toString()) != null) {
            return true;
        }

        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setElectionTimeoutMs(this.electionTimeoutMs);
        nodeOptions.setEnableMetrics(enableMetrics);
        nodeOptions.setSnapshotThrottle(snapshotThrottle);
        nodeOptions.setSnapshotIntervalSecs(snapshotIntervalSecs);
        if (raftOptions != null) {
            nodeOptions.setRaftOptions(raftOptions);
        }
        final String serverDataPath = this.dataPath + File.separator + listenAddr.toString().replace(':', '_');
        FileUtils.forceMkdir(new File(serverDataPath));
        nodeOptions.setLogUri(serverDataPath + File.separator + "logs");
        nodeOptions.setRaftMetaUri(serverDataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(serverDataPath + File.separator + "snapshot");
        final MockStateMachine fsm = new MockStateMachine(listenAddr);
        nodeOptions.setFsm(fsm);

        if (!emptyPeers) {
            nodeOptions.setInitialConf(new Configuration(this.peers));
        }

        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(listenAddr);
        final RaftGroupService server = new RaftGroupService(this.name, new PeerId(listenAddr, 0), nodeOptions,
            rpcServer);

        lock.lock();
        try {
            if (this.serverMap.put(listenAddr.toString(), server) == null) {
                final Node node = server.start();

                this.fsms.add(fsm);
                this.nodes.add((NodeImpl) node);
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    public List<MockStateMachine> getFsms() {
        lock.lock();
        try {
            return new ArrayList<>(this.fsms);
        } finally {
            lock.unlock();
        }
    }

    public boolean stop(Endpoint listenAddr) throws InterruptedException {
        final Node node = this.removeNode(listenAddr);
        final CountDownLatch latch = new CountDownLatch(1);
        if (node != null) {
            node.shutdown(new ExpectClosure(latch));
            latch.await();
            node.join();
        }
        final RaftGroupService raftGroupService = this.serverMap.remove(listenAddr.toString());
        raftGroupService.shutdown();
        raftGroupService.join();
        return node != null;
    }

    public void stopAll() throws InterruptedException {
        final List<Endpoint> addrs = this.getAllNodes();
        final List<Node> nodes = new ArrayList<>();
        for (final Endpoint addr : addrs) {
            final Node node = removeNode(addr);
            node.shutdown();
            nodes.add(node);
            serverMap.remove(addr.toString()).shutdown();
        }
        for (final Node node : nodes) {
            node.join();
        }
    }

    public void clean(Endpoint listenAddr) throws IOException {
        final String path = this.dataPath + File.separator + listenAddr.toString().replace(':', '_');
        System.out.println("Clean dir:" + path);
        FileUtils.deleteDirectory(new File(path));
    }

    public Node getLeader() {
        lock.lock();
        try {
            for (int i = 0; i < nodes.size(); i++) {
                final NodeImpl node = nodes.get(i);
                if (node.isLeader() && fsms.get(i).getLeaderTerm() == node.getCurrentTerm()) {
                    return node;
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    public MockStateMachine getLeaderFsm() {
        final Node leader = getLeader();
        if (leader != null) {
            return (MockStateMachine) leader.getOptions().getFsm();
        }
        return null;
    }

    public void waitLeader() throws InterruptedException {
        while (true) {
            final Node node = getLeader();
            if (node != null) {
                return;
            } else {
                Thread.sleep(10);
            }
        }
    }

    public List<Node> getFollowers() {
        final List<Node> ret = new ArrayList<>();
        lock.lock();
        try {
            for (final NodeImpl node : this.nodes) {
                if (!node.isLeader()) {
                    ret.add(node);
                }
            }
        } finally {
            lock.unlock();
        }
        return ret;
    }

    /**
     * Ensure all peers leader is expectAddr
     * @param expectAddr expected address
     * @throws InterruptedException if interrupted
     */
    public void ensureLeader(Endpoint expectAddr) throws InterruptedException {
        while (true) {
            lock.lock();
            for (final Node node : this.nodes) {
                final PeerId leaderId = node.getLeaderId();
                if (!leaderId.getEndpoint().equals(expectAddr)) {
                    lock.unlock();
                    Thread.sleep(10);
                    continue;
                }
            }
            // all is ready
            lock.unlock();
            return;
        }
    }

    public List<NodeImpl> getNodes() {
        lock.lock();
        try {
            return new ArrayList<>(this.nodes);
        } finally {
            lock.unlock();
        }
    }

    public List<Endpoint> getAllNodes() {
        lock.lock();
        try {
            return this.nodes.stream().map(node -> node.getNodeId().getPeerId().getEndpoint())
                    .collect(Collectors.toList());
        } finally {
            lock.unlock();
        }
    }

    public Node removeNode(Endpoint addr) {
        Node ret = null;
        lock.lock();
        try {
            for (int i = 0; i < nodes.size(); i++) {
                if (nodes.get(i).getNodeId().getPeerId().getEndpoint().equals(addr)) {
                    ret = nodes.remove(i);
                    fsms.remove(i);
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
        return ret;
    }

    public boolean ensureSame() throws InterruptedException {
        return this.ensureSame(-1);
    }

    /**
     * Ensure all logs is the same in all nodes.
     * @param waitTimes
     * @return
     * @throws InterruptedException
     */
    public boolean ensureSame(int waitTimes) throws InterruptedException {
        lock.lock();
        if (fsms.size() <= 1) {
            lock.unlock();
            return true;
        }
        try {
            int nround = 0;
            final MockStateMachine first = fsms.get(0);
            CHECK: while (true) {
                first.lock();
                if (first.getLogs().isEmpty()) {
                    first.unlock();
                    Thread.sleep(10);
                    nround++;
                    if (waitTimes > 0 && nround > waitTimes) {
                        return false;
                    }
                    continue CHECK;
                }

                for (int i = 1; i < fsms.size(); i++) {
                    final MockStateMachine fsm = fsms.get(i);
                    fsm.lock();
                    if (fsm.getLogs().size() != first.getLogs().size()) {
                        fsm.unlock();
                        first.unlock();
                        Thread.sleep(10);
                        nround++;
                        if (waitTimes > 0 && nround > waitTimes) {
                            return false;
                        }
                        continue CHECK;
                    }

                    for (int j = 0; j < first.getLogs().size(); j++) {
                        final ByteBuffer firstData = first.getLogs().get(j);
                        final ByteBuffer fsmData = fsm.getLogs().get(j);
                        if (!firstData.equals(fsmData)) {
                            fsm.unlock();
                            first.unlock();
                            Thread.sleep(10);
                            nround++;
                            if (waitTimes > 0 && nround > waitTimes) {
                                return false;
                            }
                            continue CHECK;
                        }
                    }
                    fsm.unlock();
                }
                first.unlock();
                break;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }
}
