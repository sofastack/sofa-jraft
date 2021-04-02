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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.util.MutableHandlerRegistry;

import com.alipay.sofa.jraft.rpc.RaftRpcFactory;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SPI;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.google.protobuf.Message;

/**
 * @author nicholas.jxf
 * @author jiachun.fjc
 */
@SPI(priority = 1)
public class GrpcRaftRpcFactory implements RaftRpcFactory {

    static final String             FIXED_METHOD_NAME              = "_call";
    static final int                RPC_SERVER_PROCESSOR_POOL_SIZE = SystemPropertyUtil
                                                                       .getInt(
                                                                           "jraft.grpc.default_rpc_server_processor_pool_size",
                                                                           100);

    static final int                RPC_MAX_INBOUND_MESSAGE_SIZE   = SystemPropertyUtil.getInt(
                                                                       "jraft.grpc.max_inbound_message_size.bytes",
                                                                       4 * 1024 * 1024);

    static final RpcResponseFactory RESPONSE_FACTORY               = new GrpcResponseFactory();

    final Map<String, Message>      parserClasses                  = new ConcurrentHashMap<>();
    final MarshallerRegistry        defaultMarshallerRegistry      = new MarshallerRegistry() {

                                                                       @Override
                                                                       public Message findResponseInstanceByRequest(final String reqCls) {
                                                                           return MarshallerHelper
                                                                               .findRespInstance(reqCls);
                                                                       }

                                                                       @Override
                                                                       public void registerResponseInstance(final String reqCls,
                                                                                                            final Message respIns) {
                                                                           MarshallerHelper.registerRespInstance(
                                                                               reqCls, respIns);
                                                                       }
                                                                   };

    @Override
    public void registerProtobufSerializer(final String className, final Object... args) {
        this.parserClasses.put(className, (Message) args[0]);
    }

    @Override
    public RpcClient createRpcClient(final ConfigHelper<RpcClient> helper) {
        final RpcClient rpcClient = new GrpcClient(this.parserClasses, getMarshallerRegistry());
        if (helper != null) {
            helper.config(rpcClient);
        }
        return rpcClient;
    }

    @Override
    public RpcServer createRpcServer(final Endpoint endpoint, final ConfigHelper<RpcServer> helper) {
        final int port = Requires.requireNonNull(endpoint, "endpoint").getPort();
        Requires.requireTrue(port > 0 && port < 0xFFFF, "port out of range:" + port);
        final MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry();
        final Server server = ServerBuilder.forPort(port) //
            .fallbackHandlerRegistry(handlerRegistry) //
            .directExecutor() //
            .maxInboundMessageSize(RPC_MAX_INBOUND_MESSAGE_SIZE) //
            .build();
        final RpcServer rpcServer = new GrpcServer(server, handlerRegistry, this.parserClasses, getMarshallerRegistry());
        if (helper != null) {
            helper.config(rpcServer);
        }
        return rpcServer;
    }

    @Override
    public RpcResponseFactory getRpcResponseFactory() {
        return RESPONSE_FACTORY;
    }

    @Override
    public boolean isReplicatorPipelineEnabled() {
        return true;
    }

    public MarshallerRegistry getMarshallerRegistry() {
        return defaultMarshallerRegistry;
    }
}
