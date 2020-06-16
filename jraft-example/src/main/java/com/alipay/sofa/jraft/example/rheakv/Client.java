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

import java.util.List;

import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.cmd.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.configured.MultiRegionRouteTableOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

/**
 *
 * @author jiachun.fjc
 */
public class Client {

    private final RheaKVStore rheaKVStore = new DefaultRheaKVStore();

    public void init() {
        final List<RegionRouteTableOptions> regionRouteTableOptionsList = MultiRegionRouteTableOptionsConfigured
            .newConfigured() //
            .withInitialServerList(-1L /* default id */, Configs.ALL_NODE_ADDRESSES) //
            .config();
        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured() //
            .withFake(true) //
            .withRegionRouteTableOptionsList(regionRouteTableOptionsList) //
            .config();
        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured() //
            .withClusterName(Configs.CLUSTER_NAME) //
            .withPlacementDriverOptions(pdOpts) //
            .config();
        System.out.println(opts);

        // 注册 request 和 response proto
        RpcFactoryHelper.rpcFactory().registerProtobufSerializer(RheakvRpc.BaseRequest.class.getName(),
            RheakvRpc.BaseRequest.getDefaultInstance());
        RpcFactoryHelper.rpcFactory().registerProtobufSerializer(RheakvRpc.BaseResponse.class.getName(),
            RheakvRpc.BaseResponse.getDefaultInstance());

        // 注册 request 和response 的映射关系
        MarshallerHelper.registerRespInstance(RheakvRpc.BaseRequest.class.getName(),
            RheakvRpc.BaseResponse.getDefaultInstance());

        rheaKVStore.init(opts);
    }

    public void shutdown() {
        this.rheaKVStore.shutdown();
    }

    public RheaKVStore getRheaKVStore() {
        return rheaKVStore;
    }
}
