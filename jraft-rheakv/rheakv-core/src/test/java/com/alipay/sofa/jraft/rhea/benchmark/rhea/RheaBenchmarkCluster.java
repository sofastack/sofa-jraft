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
package com.alipay.sofa.jraft.rhea.benchmark.rhea;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;

import com.alipay.remoting.config.Configs;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.errors.NotLeaderException;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 *
 * @author jiachun.fjc
 */
public class RheaBenchmarkCluster {

    private static final String[]             CONF   = {
            "jraft-rheakv/rheakv-core/src/test/resources/benchmark/conf/rhea_cluster_1.yaml",
            "jraft-rheakv/rheakv-core/src/test/resources/benchmark/conf/rhea_cluster_2.yaml",
            "jraft-rheakv/rheakv-core/src/test/resources/benchmark/conf/rhea_cluster_3.yaml" };

    private volatile String                   tempDbPath;
    private volatile String                   tempRaftPath;
    private CopyOnWriteArrayList<RheaKVStore> stores = new CopyOnWriteArrayList<>();

    protected void start() throws IOException, InterruptedException {
        SystemPropertyUtil.setProperty(Configs.NETTY_BUFFER_LOW_WATERMARK, Integer.toString(256 * 1024));
        SystemPropertyUtil.setProperty(Configs.NETTY_BUFFER_HIGH_WATERMARK, Integer.toString(512 * 1024));
        File file = new File("benchmark_rhea_db");
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        file = new File("benchmark_rhea_db");
        if (file.mkdir()) {
            this.tempDbPath = file.getAbsolutePath();
            System.out.println("make dir: " + this.tempDbPath);
        }
        file = new File("benchmark_rhea_raft");
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        file = new File("benchmark_rhea_raft");
        if (file.mkdir()) {
            this.tempRaftPath = file.getAbsolutePath();
            System.out.println("make dir: " + this.tempRaftPath);
        }
        for (String c : CONF) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            final RheaKVStoreOptions opts = mapper.readValue(new File(c), RheaKVStoreOptions.class);
            RheaKVStore rheaKVStore = new DefaultRheaKVStore();
            if (rheaKVStore.init(opts)) {
                stores.add(rheaKVStore);
            } else {
                throw new RuntimeException("Fail to init rhea kv store witch conf: " + c);
            }
        }
        PlacementDriverClient pdClient = stores.get(0).getPlacementDriverClient();
        Endpoint leader1 = pdClient.getLeader(1, true, 10000);
        System.out.println("The region 1 leader is: " + leader1);
        //        Endpoint leader2 = pdClient.getLeader(2, true, 10000);
        //        System.out.println("The region 2 leader is: " + leader2);
        //        Endpoint leader3 = pdClient.getLeader(3, true, 10000);
        //        System.out.println("The region 3 leader is: " + leader3);
    }

    protected void shutdown() throws IOException {
        for (RheaKVStore store : stores) {
            store.shutdown();
        }
        if (this.tempDbPath != null) {
            System.out.println("removing dir: " + this.tempDbPath);
            FileUtils.forceDelete(new File(this.tempDbPath));
        }
        if (this.tempRaftPath != null) {
            System.out.println("removing dir: " + this.tempRaftPath);
            FileUtils.forceDelete(new File(this.tempRaftPath));
        }
    }

    public RheaKVStore getLeaderStore(long regionId) {
        for (int i = 0; i < 20; i++) {
            for (RheaKVStore store : stores) {
                if (((DefaultRheaKVStore) store).isLeader(regionId)) {
                    return store;
                }
            }
            System.out.println("fail to find leader, try again");
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
        throw new NotLeaderException("no leader");
    }

    public RheaKVStore getFollowerStore(long regionId) {
        for (int i = 0; i < 20; i++) {
            for (RheaKVStore store : stores) {
                if (!((DefaultRheaKVStore) store).isLeader(regionId)) {
                    return store;
                }
            }
            System.out.println("fail to find follower, try again");
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
        throw new NotLeaderException("no follower");
    }
}
