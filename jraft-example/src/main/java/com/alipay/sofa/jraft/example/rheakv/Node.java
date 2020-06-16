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

import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.cmd.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rpc.impl.GrpcServer;
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.ExtensionRegistry;
import io.grpc.protobuf.ProtoUtils;

/**
 *
 * @author jiachun.fjc
 */
public class Node {

    private final RheaKVStoreOptions options;

    private RheaKVStore              rheaKVStore;

    public Node(RheaKVStoreOptions options) {
        this.options = options;
    }

    public void start() {

        // 注册 request 和 response proto
        RpcFactoryHelper.rpcFactory().registerProtobufSerializer(RheakvRpc.BaseRequest.class.getName(),
            RheakvRpc.BaseRequest.getDefaultInstance());
        RpcFactoryHelper.rpcFactory().registerProtobufSerializer(RheakvRpc.BaseResponse.class.getName(),
            RheakvRpc.BaseResponse.getDefaultInstance());

        // 注册 request 和response 的映射关系
        MarshallerHelper.registerRespInstance(RheakvRpc.BaseRequest.class.getName(),
            RheakvRpc.BaseResponse.getDefaultInstance());

        // GRPC注册反序列化注册机
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        RheakvRpc.registerAllExtensions(registry);
        ProtoUtils.setExtensionRegistry(registry);

        this.rheaKVStore = new DefaultRheaKVStore();
        this.rheaKVStore.init(this.options);
    }

    public void stop() {
        this.rheaKVStore.shutdown();
    }

    public RheaKVStore getRheaKVStore() {
        return rheaKVStore;
    }
}
