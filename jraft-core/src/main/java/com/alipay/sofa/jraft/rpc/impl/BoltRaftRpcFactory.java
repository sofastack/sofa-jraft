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
package com.alipay.sofa.jraft.rpc.impl;

import com.alipay.remoting.CustomSerializerManager;
import com.alipay.remoting.InvokeContext;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.ProtobufSerializer;
import com.alipay.sofa.jraft.rpc.RaftRpcFactory;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.core.JRaftRpcAddressParser;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SPI;

/**
 *
 * @author jiachun.fjc
 */
@SPI
public class BoltRaftRpcFactory implements RaftRpcFactory {

    @Override
    public void registerProtobufSerializer(final String className) {
        CustomSerializerManager.registerCustomSerializer(className, ProtobufSerializer.INSTANCE);
    }

    @Override
    public RpcClient createRpcClient(final ConfigHelper<RpcClient> helper) {
        final com.alipay.remoting.rpc.RpcClient boltImpl = new com.alipay.remoting.rpc.RpcClient();
        final RpcClient rpcClient = new BoltRpcClient(boltImpl);
        if (helper != null) {
            helper.config(rpcClient);
        }
        return rpcClient;
    }

    @Override
    public RpcServer createRpcServer(final Endpoint endpoint, final ConfigHelper<RpcServer> helper) {
        final int port = Requires.requireNonNull(endpoint, "endpoint").getPort();
        Requires.requireTrue(port > 0 && port < 0xFFFF, "port out of range:" + port);
        final com.alipay.remoting.rpc.RpcServer boltImpl = new com.alipay.remoting.rpc.RpcServer(port, true, false);
        final RpcServer rpcServer = new BoltRpcServer(boltImpl);
        if (helper != null) {
            helper.config(rpcServer);
        }
        return rpcServer;
    }

    @Override
    public ConfigHelper<RpcClient> defaultJRaftClientConfigHelper(final RpcOptions opts) {
        return instance -> {
            final BoltRpcClient client = (BoltRpcClient) instance;
            final InvokeContext ctx = new InvokeContext();
            ctx.put(InvokeContext.BOLT_CRC_SWITCH, opts.isEnableRpcChecksum());
            client.setDefaultInvokeCtx(ctx);
            client.setDefaultAddressParser(new JRaftRpcAddressParser());
        };
    }
}
