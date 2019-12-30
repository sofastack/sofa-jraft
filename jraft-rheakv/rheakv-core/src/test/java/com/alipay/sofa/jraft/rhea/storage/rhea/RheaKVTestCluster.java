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
package com.alipay.sofa.jraft.rhea.storage.rhea;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.errors.NotLeaderException;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 *
 * @author jiachun.fjc
 */
public class RheaKVTestCluster {

    private static final Logger   LOG            = LoggerFactory.getLogger(RheaKVTestCluster.class);

    public static String          DB_PATH        = "rhea_db";
    public static String          RAFT_DATA_PATH = "rhea_raft";
    public static Long[]          REGION_IDS     = new Long[] { 1L, 2L };

    private static final String[] CONF           = { "/conf/rhea_test_cluster_1.yaml",
            "/conf/rhea_test_cluster_2.yaml", "/conf/rhea_test_cluster_3.yaml" };

    private List<RheaKVStore>     stores         = new CopyOnWriteArrayList<>();

    protected void start(final StorageType storageType) throws Exception {
        start(storageType, true);
    }

    protected void start(final StorageType storageType, final boolean deleteFiles) throws Exception {
        if (deleteFiles) {
            deleteFiles();
        }
        for (final String c : CONF) {
            final RheaKVStoreOptions opts = readOpts(c);
            opts.getStoreEngineOptions().setStorageType(storageType);
            final RheaKVStore rheaKVStore = new DefaultRheaKVStore();
            if (rheaKVStore.init(opts)) {
                stores.add(rheaKVStore);
            } else {
                throw new RuntimeException("Fail to init rhea kv store witch conf: " + c);
            }
        }
        for (final Long id : REGION_IDS) {
            getLeaderStore(id);
        }
    }

    protected void shutdown() throws Exception {
        shutdown(true);
    }

    protected void shutdown(final boolean deleteFiles) throws Exception {
        for (final RheaKVStore store : stores) {
            store.shutdown();
        }
        stores.clear();
        if (deleteFiles) {
            deleteFiles();
        }
        LOG.info("RheaKVTestCluster shutdown complete");
    }

    private RheaKVStoreOptions readOpts(final String conf) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try (final InputStream in = RheaKVTestCluster.class.getResourceAsStream(conf)) {
            return mapper.readValue(in, RheaKVStoreOptions.class);
        }
    }

    protected RheaKVStore getRandomLeaderStore() {
        return getLeaderStore(getRandomRegionId());
    }

    protected RheaKVStore getLeaderStore(final long regionId) {
        for (int i = 0; i < 100; i++) {
            for (final RheaKVStore store : stores) {
                if (((DefaultRheaKVStore) store).isLeader(regionId)) {
                    return store;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
        throw new NotLeaderException("no leader on region: " + regionId);
    }

    protected RheaKVStore getRandomFollowerStore() {
        return getFollowerStore(getRandomRegionId());
    }

    protected RheaKVStore getFollowerStore(final long regionId) {
        for (int i = 0; i < 100; i++) {
            final List<RheaKVStore> tmp = Lists.newArrayList();
            for (final RheaKVStore store : stores) {
                if (!((DefaultRheaKVStore) store).isLeader(regionId)) {
                    // maybe a learner
                    tmp.add(store);
                }
            }
            if (!tmp.isEmpty()) {
                return tmp.get(ThreadLocalRandom.current().nextInt(tmp.size()));
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
        throw new NotLeaderException("no follower/learner");
    }

    protected Long getRandomRegionId() {
        return REGION_IDS[ThreadLocalRandom.current().nextInt(REGION_IDS.length)];
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
}
