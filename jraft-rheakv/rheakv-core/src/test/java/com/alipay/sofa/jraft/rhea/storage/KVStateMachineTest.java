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
package com.alipay.sofa.jraft.rhea.storage;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rhea.StateListenerContainer;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.client.pd.FakePlacementDriverClient;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;

import static com.alipay.sofa.jraft.core.State.STATE_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jiachun.fjc
 */
public class KVStateMachineTest {

    private static final int APPLY_COUNT   = 100;
    private static final int SUCCESS_COUNT = 10;

    private RaftGroupService raftGroupService;
    private RaftRawKVStore   raftRawKVStore;
    private File             raftDataPath;

    @Before
    public void setup() throws IOException, InterruptedException {
        final Region region = new Region();
        region.setId(1);
        final StoreEngine storeEngine = new MockStoreEngine();
        final KVStoreStateMachine fsm = new KVStoreStateMachine(region, storeEngine);
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

    @After
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
    }

    @Test
    public void failApplyTest() throws Exception {
        final CountDownLatch latch = new CountDownLatch(APPLY_COUNT);
        final List<KVStoreClosure> closures = new ArrayList<>();
        final BlockingQueue<Status> successQueue = new ArrayBlockingQueue<>(APPLY_COUNT);
        final BlockingQueue<Status> failQueue = new ArrayBlockingQueue<>(APPLY_COUNT);
        assertTrue(this.raftGroupService.getRaftNode().isLeader());
        for (int i = 0; i < SUCCESS_COUNT; i++) {
            final KVStoreClosure c = new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
                    successQueue.add(status);
                    latch.countDown();
                }
            };
            closures.add(c);
        }
        for (int i = SUCCESS_COUNT; i < APPLY_COUNT; i++) {
            final KVStoreClosure c = new BaseKVStoreClosure() {

                @Override
                public void run(Status status) {
                    failQueue.add(status);
                    latch.countDown();
                }
            };
            closures.add(c);
        }

        for (int i = 0; i < SUCCESS_COUNT; i++) {
            final byte[] bytes = BytesUtil.writeUtf8(String.valueOf(i));
            this.raftRawKVStore.put(bytes, bytes, closures.get(i));
        }

        for (int i = SUCCESS_COUNT; i < APPLY_COUNT; i++) {
            final byte[] bytes = BytesUtil.writeUtf8(String.valueOf(i));
            this.raftRawKVStore.merge(bytes, bytes, closures.get(i));
        }

        latch.await();

        final Node node = this.raftGroupService.getRaftNode();
        assertFalse(node.isLeader());
        final Field field = node.getClass().getDeclaredField("state");
        field.setAccessible(true);
        assertEquals(field.get(node), STATE_ERROR);
        assertEquals(SUCCESS_COUNT, successQueue.size());
        assertEquals(APPLY_COUNT - SUCCESS_COUNT, failQueue.size());
        while (true) {
            final Status st = successQueue.poll();
            if (st == null) {
                break;
            }
            assertTrue(st.isOk());
        }

        while (true) {
            final Status st = failQueue.poll();
            if (st == null) {
                break;
            }
            assertFalse(st.isOk());
            assertTrue(st.getRaftError() == RaftError.ESTATEMACHINE || st.getRaftError() == RaftError.EPERM);
        }
    }

    static class MockKVStore extends MemoryRawKVStore {

        private int putIndex = 0;

        @Override
        public void put(byte[] key, byte[] value, KVStoreClosure closure) {
            if (this.putIndex++ < SUCCESS_COUNT) {
                if (closure != null) {
                    closure.setData(value);
                    closure.run(Status.OK());
                }
            } else {
                throw new RuntimeException("fail put test");
            }
        }

        @Override
        public void merge(byte[] key, byte[] value, KVStoreClosure closure) {
            if (this.putIndex++ < SUCCESS_COUNT) {
                if (closure != null) {
                    closure.setData(value);
                    closure.run(Status.OK());
                }
            } else {
                throw new RuntimeException("fail merge test");
            }
        }

        @Override
        void doSnapshotSave(MemoryKVStoreSnapshotFile snapshotFile, String snapshotPath, Region region)
                                                                                                       throws Exception {
            super.doSnapshotSave(snapshotFile, snapshotPath, region);
            snapshotFile.writeToFile(snapshotPath, "putIndex", new PutIndex(this.putIndex));
        }

        @Override
        void doSnapshotLoad(MemoryKVStoreSnapshotFile snapshotFile, String snapshotPath) throws Exception {
            super.doSnapshotLoad(snapshotFile, snapshotPath);
            final PutIndex p = snapshotFile.readFromFile(snapshotPath, "putIndex", PutIndex.class);
            this.putIndex = p.data();
        }

        class PutIndex extends MemoryKVStoreSnapshotFile.Persistence<Integer> {

            public PutIndex(Integer data) {
                super(data);
            }
        }
    }

    static class MockStoreEngine extends StoreEngine {

        private final MockKVStore     mockKVStore        = new MockKVStore();
        private final ExecutorService leaderStateTrigger = Executors.newSingleThreadExecutor();

        public MockStoreEngine() {
            super(new MockPlacementDriverClient(), new StateListenerContainer<>());
        }

        @Override
        public BatchRawKVStore<?> getRawKVStore() {
            return this.mockKVStore;
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
