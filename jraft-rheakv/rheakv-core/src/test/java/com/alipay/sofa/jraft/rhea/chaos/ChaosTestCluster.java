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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.TestUtil;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.errors.NotLeaderException;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.options.BatchingOptions;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RocksDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.StoreEngineOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.rhea.util.Constants;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * @author jiachun.fjc
 */
public class ChaosTestCluster {

    private static final Logger     LOG            = LoggerFactory.getLogger(ChaosTestCluster.class);

    public static String            CLUSTER_NAME   = "chaos_test";
    public static String            DB_PATH        = "chaos_db" + File.separator;
    public static String            RAFT_DATA_PATH = "chaos_raft_data" + File.separator;

    private final List<PeerId>      peerIds;
    private final StorageType       storageType;
    private final boolean           allowBatching;
    private final boolean           onlyLeaderRead;
    private final List<RheaKVStore> stores         = new CopyOnWriteArrayList<>();

    public ChaosTestCluster(List<PeerId> peerIds, StorageType storageType, boolean allowBatching, boolean onlyLeaderRead) {
        this.peerIds = peerIds;
        this.storageType = storageType;
        this.allowBatching = allowBatching;
        this.onlyLeaderRead = onlyLeaderRead;
    }

    public synchronized void start() {
        deleteFiles();
        final Configuration conf = new Configuration(this.peerIds);
        final String initialServerList = conf.toString();
        for (final PeerId p : conf.listPeers()) {
            final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured().withFake(true) // use a fake pd
                .config();
            final StoreEngineOptions storeOpts = StoreEngineOptionsConfigured.newConfigured() //
                .withStorageType(this.storageType) //
                .withRocksDBOptions(RocksDBOptionsConfigured.newConfigured().withDbPath(DB_PATH).config()) //
                .withRaftDataPath(RAFT_DATA_PATH) //
                .withServerAddress(p.getEndpoint()) //
                .withLeastKeysOnSplit(10) //
                .config();
            final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured() //
                .withClusterName(CLUSTER_NAME) //
                .withInitialServerList(initialServerList).withOnlyLeaderRead(this.onlyLeaderRead) //
                .withStoreEngineOptions(storeOpts) //
                .withPlacementDriverOptions(pdOpts) //
                .withFailoverRetries(30) //
                .withFutureTimeoutMillis(TimeUnit.SECONDS.toMillis(60)) //
                .config();
            BatchingOptions batchingOptions = opts.getBatchingOptions();
            if (batchingOptions == null) {
                batchingOptions = new BatchingOptions();
            }
            batchingOptions.setAllowBatching(this.allowBatching);
            opts.setBatchingOptions(batchingOptions);

            final RheaKVStore store = new DefaultRheaKVStore();
            if (!store.init(opts)) {
                throw new IllegalStateException("fail to init store with options: " + opts);
            }
            this.stores.add(store);
        }
        awaitLeader();
    }

    public synchronized void stopAll() {
        for (final RheaKVStore store : this.stores) {
            store.shutdown();
        }
        deleteFiles();
    }

    private void deleteFiles() {
        final File dbFile = new File(DB_PATH);
        if (dbFile.exists()) {
            try {
                FileUtils.forceDelete(dbFile);
                LOG.info("delete db file: {}", dbFile.getAbsolutePath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        final File raftFile = new File(RAFT_DATA_PATH);
        if (raftFile.exists()) {
            try {
                FileUtils.forceDelete(raftFile);
                LOG.info("remove raft data: {}", raftFile.getAbsolutePath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized RheaKVStore getLeaderStore() {
        awaitLeader();
        for (final RheaKVStore store : this.stores) {
            if (((DefaultRheaKVStore) store).isLeader(Constants.DEFAULT_REGION_ID)) {
                return store;
            }
        }
        throw new NotLeaderException("no leader");
    }

    public synchronized RheaKVStore getByStorePeer(final PeerId peerId) {
        awaitLeader();
        final Endpoint endpoint = JRaftHelper.toPeer(peerId).getEndpoint();
        for (final RheaKVStore store : this.stores) {
            if (endpoint.equals(getSelfEndpoint(store))) {
                return store;
            }
        }
        throw new RuntimeException("fail to get peer: " + peerId);
    }

    public synchronized void removePeer(final PeerId peerId) {
        for (int i = this.stores.size() - 1; i >= 0; i--) {
            final RheaKVStore store = this.stores.get(i);
            if (peerId.getEndpoint().equals(getSelfEndpoint(store))) {
                final PlacementDriverClient pdClient = store.getPlacementDriverClient();
                if (!pdClient.removeReplica(Constants.DEFAULT_REGION_ID, JRaftHelper.toPeer(peerId), true)) {
                    throw new RuntimeException("fail to remove peer: " + peerId);
                }
                store.shutdown();
                this.stores.remove(i);
                this.peerIds.remove(i);
                LOG.info("Shutdown and remove peer: {}", peerId);
                return;
            }
        }
        LOG.info("Could not find peer: {}", peerId);
    }

    public synchronized void addPeer(final PeerId peerId) {
        if (this.peerIds.contains(peerId)) {
            throw new RuntimeException("peerId is exist: " + peerId);
        }
        this.peerIds.add(peerId);
        final Configuration conf = new Configuration(this.peerIds);
        final String initialServerList = conf.toString();
        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured().withFake(true) // use a fake pd
            .config();
        final StoreEngineOptions storeOpts = StoreEngineOptionsConfigured.newConfigured() //
            .withStorageType(this.storageType) //
            .withRocksDBOptions(RocksDBOptionsConfigured.newConfigured().withDbPath(DB_PATH).config()) //
            .withRaftDataPath(RAFT_DATA_PATH) //
            .withServerAddress(peerId.getEndpoint()) //
            .config();
        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured() //
            .withClusterName("chaos_test") //
            .withInitialServerList(initialServerList).withStoreEngineOptions(storeOpts) //
            .withPlacementDriverOptions(pdOpts) //
            .config();
        BatchingOptions batchingOptions = opts.getBatchingOptions();
        if (batchingOptions == null) {
            batchingOptions = new BatchingOptions();
        }
        batchingOptions.setAllowBatching(this.allowBatching);
        opts.setBatchingOptions(batchingOptions);

        final RheaKVStore store = new DefaultRheaKVStore();
        if (!store.init(opts)) {
            throw new IllegalStateException("fail to init store with options: " + opts);
        }

        final RheaKVStore leader = getLeaderStore();
        final PlacementDriverClient pdClient = leader.getPlacementDriverClient();
        if (!pdClient.addReplica(Constants.DEFAULT_REGION_ID, JRaftHelper.toPeer(peerId), true)) {
            throw new RuntimeException("fail to add peer: " + peerId);
        }
        this.stores.add(store);
        awaitLeader();
    }

    public synchronized RheaKVStore getRandomStore() {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        return this.stores.get(random.nextInt(this.stores.size()));
    }

    public synchronized PeerId getRandomPeer() {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        return this.peerIds.get(random.nextInt(this.peerIds.size()));
    }

    public synchronized void randomTransferLeader() {
        final RheaKVStore leader = getLeaderStore();
        final Endpoint leaderEndpoint = getSelfEndpoint(leader);
        final PlacementDriverClient pdClient = leader.getPlacementDriverClient();
        final Peer randomPeer = JRaftHelper.toPeer(getRandomPeer());
        boolean result = pdClient.transferLeader(Constants.DEFAULT_REGION_ID, randomPeer, false);
        if (!result) {
            throw new RuntimeException("fail to transfer leader [" + leaderEndpoint + " --> " + randomPeer);
        }
        LOG.info("Transfer leader from {} to {}", leaderEndpoint, randomPeer.getEndpoint());
    }

    public synchronized Endpoint getSelfEndpoint(final RheaKVStore store) {
        final StoreEngine storeEngine = TestUtil.getByName(store, "storeEngine", StoreEngine.class);
        return storeEngine.getSelfEndpoint();
    }

    public synchronized void awaitLeader() {
        for (int i = 0; i < 100; i++) {
            for (final RheaKVStore store : this.stores) {
                if (((DefaultRheaKVStore) store).isLeader(Constants.DEFAULT_REGION_ID)) {
                    return;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
        throw new NotLeaderException("wait leader timeout");
    }
}
