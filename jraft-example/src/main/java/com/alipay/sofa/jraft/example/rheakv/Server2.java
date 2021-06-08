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
package com.alipay.sofa.jraft.example.rheakv;

import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RocksDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.StoreEngineOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 *
 * @author jiachun.fjc
 */
public class Server2 {

    public static void main(final String[] args) throws Exception {
        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured()
                .withFake(true) // use a fake pd
                .config();
        final StoreEngineOptions storeOpts = StoreEngineOptionsConfigured.newConfigured() //
                .withStorageType(StorageType.RocksDB)
                .withRocksDBOptions(RocksDBOptionsConfigured.newConfigured().withDbPath(Configs.DB_PATH).config())
                .withRaftDataPath(Configs.RAFT_DATA_PATH)
                .withServerAddress(new Endpoint("127.0.0.1", 8182))
                .config();
        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured() //
                .withClusterName(Configs.CLUSTER_NAME) //
                .withUseParallelCompress(true) //
                .withInitialServerList(Configs.ALL_NODE_ADDRESSES)
                .withStoreEngineOptions(storeOpts) //
                .withPlacementDriverOptions(pdOpts) //
                .config();
        System.out.println(opts);
        final Node node = new Node(opts);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        System.out.println("server2 start OK");
    }
}
