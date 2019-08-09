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
package com.alipay.sofa.jraft.rhea.chaos;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.RheaKVServiceFactory;
import com.alipay.sofa.jraft.rhea.TestUtil;
import com.alipay.sofa.jraft.rhea.client.FutureGroup;
import com.alipay.sofa.jraft.rhea.client.RheaKVCliService;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.rhea.util.Constants;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Utils;

/**
 *
 * @author jiachun.fjc
 */
public abstract class AbstractChaosTest {

    private static final int    LOOP_1             = Utils.cpus();
    private static final int    LOOP_2             = 20;
    private static final int    INITIAL_PEER_COUNT = 5;
    private static final int    RETRIES            = 10;
    private static final byte[] VALUE              = BytesUtil.writeUtf8("test");

    @Test
    public void chaosGetTest() throws Exception {
        ChaosTestCluster cluster = null;
        PeerId p1 = null;
        PeerId p2 = null;
        for (int l = 0; l < RETRIES; l++) {
            final ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("chaos-test", true));
            final List<CompletableFuture<Boolean>> allFutures = new CopyOnWriteArrayList<>();
            try {
                cluster = new ChaosTestCluster(TestUtil.generatePeers(INITIAL_PEER_COUNT), getStorageType(),
                        isAllowBatching(), isOnlyLeaderRead());
                cluster.start();

                // 在写入数据之前, 先移除一个节点 (node1), 后面再添加回来, 验证是否能保证读一致性
                p1 = cluster.getRandomPeer();
                cluster.removePeer(p1);

                final RheaKVStore store = cluster.getLeaderStore();
                // warm up
                store.bGet("test_key");

                for (int i = 0; i < LOOP_1; i++) {
                    final int index = i;
                    executor.execute(() -> {
                        for (int j = 0; j < LOOP_2; j++) {
                            allFutures.add(store.put("test_" + index + "_" + j, VALUE));
                        }
                    });
                }

                // 在写入数据过程中, 再移除一个节点 (node2)
                p2 = cluster.getRandomPeer();
                cluster.removePeer(p2);

                // 等待写入全部完成
                CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]))
                        .get(30, TimeUnit.SECONDS);
                break;
            } catch (final Exception e) {
                System.err.println("Fail to put data, try again...");
                e.printStackTrace();
                new FutureGroup<>(allFutures).cancel(true);
                if (cluster != null) {
                    cluster.stopAll();
                }
                cluster = null;
            } finally {
                ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
            }
        }
        if (cluster == null) {
            throw new RuntimeException("fail to put data, can not check data");
        }
        try {
            chaosGetCheckData(cluster, p2, p1);
        } finally {
            cluster.stopAll();
        }
    }

    private void chaosGetCheckData(final ChaosTestCluster cluster, final PeerId p2, final PeerId p1) {
        // 随机选一个 client 验证数据一致性
        for (int i = 0; i < LOOP_1; i++) {
            for (int j = 0; j < LOOP_2; j++) {
                Assert.assertArrayEquals(VALUE, cluster.getRandomStore().bGet("test_" + i + "_" + j));
            }
        }

        // node2 重新加入, 并在 node2 上验证读一致性
        cluster.addPeer(p2);
        for (int i = 0; i < LOOP_1; i++) {
            for (int j = 0; j < LOOP_2; j++) {
                Assert.assertArrayEquals(VALUE, cluster.getByStorePeer(p2).bGet("test_" + i + "_" + j));
            }
        }

        // node1 重新加入, 并在 node1 上验证读一致性 (node1 会从 leader 同步数据)
        cluster.addPeer(p1);
        for (int i = 0; i < LOOP_1; i++) {
            for (int j = 0; j < LOOP_2; j++) {
                Assert.assertArrayEquals(VALUE, cluster.getByStorePeer(p1).bGet("test_" + i + "_" + j));
            }
        }
    }

    @Test
    public void chaosSplittingTest() {
        final List<PeerId> peerIds = TestUtil.generatePeers(INITIAL_PEER_COUNT);
        final CliOptions opts = new CliOptions();
        opts.setTimeoutMs(30000);
        final RheaKVCliService cliService = RheaKVServiceFactory.createAndInitRheaKVCliService(opts);
        final long regionId = Constants.DEFAULT_REGION_ID;
        final long newRegionId = 2;
        final String groupId = JRaftHelper.getJRaftGroupId(ChaosTestCluster.CLUSTER_NAME, regionId);
        final Configuration conf = new Configuration(peerIds);
        ChaosTestCluster cluster = null;
        for (int l = 0; l < RETRIES; l++) {
            final ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("chaos-splitting-test", true));
            final List<Future<?>> allFutures = new CopyOnWriteArrayList<>();
            try {
                cluster = new ChaosTestCluster(peerIds, getStorageType(),
                        isAllowBatching(), isOnlyLeaderRead());
                cluster.start();

                final RheaKVStore store = cluster.getLeaderStore();
                // for least keys on split
                for (int j = 0; j < LOOP_2; j++) {
                    store.bPut(j + "_split_", VALUE);
                }

                for (int i = 0; i < LOOP_1; i++) {
                    final int index = i;
                    final Future<?> f = executor.submit(() -> {
                        for (int j = 0; j < LOOP_2; j++) {
                            store.bPut(index + "_split_test_" + j, VALUE);
                        }
                    });
                    allFutures.add(f);
                }

                final Status st = cliService.rangeSplit(regionId, newRegionId, groupId, conf);
                if (!st.isOk()) {
                    System.err.println("Status:" + st);
                    throw new RuntimeException(st.toString());
                }

                // wait for all writes finished
                for (final Future<?> f : allFutures) {
                    f.get(30, TimeUnit.SECONDS);
                }

                break;
            } catch (final Exception e) {
                System.err.println("Fail to put data, try again...");
                e.printStackTrace();
                for (final Future<?> f : allFutures) {
                    f.cancel(true);
                }
                if (cluster != null) {
                    cluster.stopAll();
                }
                cluster = null;
            } finally {
                ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
            }
        }
        if (cluster == null) {
            throw new RuntimeException("fail to put data, can not check data");
        }

        try {
            chaosSplittingCheckData(cluster);
        } finally {
            cluster.stopAll();
        }
    }

    private void chaosSplittingCheckData(final ChaosTestCluster cluster) {
        // 随机选一个 client 验证数据一致性
        for (int i = 0; i < LOOP_1; i++) {
            for (int j = 0; j < LOOP_2; j++) {
                Assert.assertArrayEquals(VALUE, cluster.getRandomStore().bGet(i + "_split_test_" + j));
            }
        }
    }

    public abstract StorageType getStorageType();

    public abstract boolean isAllowBatching();

    public abstract boolean isOnlyLeaderRead();
}
