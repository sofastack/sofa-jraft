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
package com.alipay.sofa.jraft.rhea.benchmark.statemachine;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rhea.StateListenerContainer;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.client.pd.FakePlacementDriverClient;
import com.alipay.sofa.jraft.rhea.fsm.BaseKVStateMachine;
import com.alipay.sofa.jraft.rhea.fsm.KVStoreStateMachine;
import com.alipay.sofa.jraft.rhea.fsm.ParallelKVStateMachine;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.ParallelSmrOptions;
import com.alipay.sofa.jraft.rhea.storage.BaseKVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.BatchRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.RaftRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.rocksdb.BaseKVStoreTest;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

/**
 * @author hzh (642256541@qq.com)
 */
public class KVStateMachineBenchmark extends BaseKVStoreTest {

    private static final int     APPLY_COUNT                = 10000;
    private static final boolean USE_PARALLEL_STATE_MACHINE = true;
    private BaseKVStateMachine   fsm;
    private RaftGroupService     raftGroupService;
    private RaftRawKVStore       raftRawKVStore;
    private File                 raftDataPath;

    public void setup() throws Exception {
        super.setup();
        final Region region = new Region();
        region.setId(1);
        final StoreEngine storeEngine = new MockStoreEngine(this.kvStore);
        if (USE_PARALLEL_STATE_MACHINE) {
            this.fsm = new ParallelKVStateMachine(region, storeEngine);
            ((ParallelKVStateMachine) this.fsm).init(new ParallelSmrOptions());
        } else {
            this.fsm = new KVStoreStateMachine(region, storeEngine);
        }
        final NodeOptions nodeOpts = new NodeOptions();
        final Configuration conf = new Configuration();
        conf.addPeer(PeerId.parsePeer("127.0.0.1:8081"));
        nodeOpts.setInitialConf(conf);
        nodeOpts.setFsm(fsm);

        final String raftDataPath = "raft_st_test";
        this.raftDataPath = new File(raftDataPath);
        if (this.raftDataPath.exists()) {
            FileUtils.forceDelete(this.raftDataPath);
        }
        FileUtils.forceMkdir(this.raftDataPath);

        final Path logUri = Paths.get(raftDataPath, "log");
        nodeOpts.setLogUri(logUri.toString());

        final Path meteUri = Paths.get(raftDataPath, "meta");
        nodeOpts.setRaftMetaUri(meteUri.toString());

        final Path snapshotUri = Paths.get(raftDataPath, "snapshot");
        nodeOpts.setSnapshotUri(snapshotUri.toString());

        final Endpoint serverAddress = new Endpoint("127.0.0.1", 8081);
        final PeerId serverId = new PeerId(serverAddress, 0);
        this.raftGroupService = new RaftGroupService("st_test", serverId, nodeOpts, null, true);

        final Node node = this.raftGroupService.start(false);

        for (int i = 0; i < 100; i++) {
            if (node.isLeader()) {
                break;
            }
            Thread.sleep(100);
        }

        final RawKVStore rawKVStore = storeEngine.getRawKVStore();
        this.raftRawKVStore = new RaftRawKVStore(node, rawKVStore, null);
    }

    public void tearDown() throws IOException {
        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                ThrowUtil.throwException(e);
            }
        }
        if (this.raftDataPath.exists()) {
            FileUtils.forceDelete(this.raftDataPath);
        }
        if (USE_PARALLEL_STATE_MACHINE) {
            ((ParallelKVStateMachine) this.fsm).shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        final KVStateMachineBenchmark benchmark = new KVStateMachineBenchmark();
        benchmark.setup();
        final long begin = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(APPLY_COUNT);
        final List<KVStoreClosure> closures = new ArrayList<>(APPLY_COUNT);
        assertTrue(benchmark.raftGroupService.getRaftNode().isLeader());

        for (int i = 0; i < APPLY_COUNT; i++) {
            final KVStoreClosure c = new BaseKVStoreClosure() {
                @Override
                public void run(Status status) {
                    try {
                        // Simulate time-consuming operations
                        Thread.sleep(10);
                    } catch (final InterruptedException ignored) {
                    }
                    latch.countDown();
                }
            };
            closures.add(c);
        }
        // Write
        for (int i = 0; i < APPLY_COUNT; i++) {
            final byte[] bytes = BytesUtil.writeUtf8("key-" + i);
            benchmark.raftRawKVStore.put(bytes, bytes, closures.get(i));
        }
        latch.await();
        System.out.println("cost:" + (System.currentTimeMillis() - begin));

        benchmark.tearDown();
    }

    public static class MockStoreEngine extends StoreEngine {

        private final ExecutorService leaderStateTrigger = Executors.newSingleThreadExecutor();
        private final RocksRawKVStore kvStore;

        public MockStoreEngine(final RocksRawKVStore kvStore) {
            super(new MockPlacementDriverClient(), new StateListenerContainer<>());
            this.kvStore = kvStore;
        }

        @Override
        public BatchRawKVStore<?> getRawKVStore() {
            return this.kvStore;
        }

        @Override
        public ExecutorService getRaftStateTrigger() {
            return this.leaderStateTrigger;
        }
    }

    static class MockPlacementDriverClient extends FakePlacementDriverClient {

        public MockPlacementDriverClient() {
            super(1, "st_test");
        }
    }
}