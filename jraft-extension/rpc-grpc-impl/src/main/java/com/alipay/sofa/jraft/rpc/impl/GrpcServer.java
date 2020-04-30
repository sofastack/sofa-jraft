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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.util.MutableHandlerRegistry;

import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.google.protobuf.Message;

/**
 * GRPC RPC server implement.
 *
 * @author nicholas.jxf
 * @author jiachun.fjc
 */
public class GrpcServer implements RpcServer {

    private final Endpoint               endpoint;
    private final MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry();
    private final AtomicBoolean          started         = new AtomicBoolean(false);
    private final Map<String, Message>   parserClasses;

    private Server                       server;

    public GrpcServer(Endpoint endpoint, Map<String, Message> parserClasses) {
        this.endpoint = endpoint;
        this.parserClasses = parserClasses;
    }

    @Override
    public boolean init(final Void opts) {
        if (!this.started.compareAndSet(false, true)) {
            throw new IllegalStateException("grpc server has started");
        }
        try {
            this.server = NettyServerBuilder //
                .forAddress(new InetSocketAddress(this.endpoint.getIp(), this.endpoint.getPort())) //
                .fallbackHandlerRegistry(this.handlerRegistry) //
                .build();
            this.server.start();
        } catch (final IOException e) {
            ThrowUtil.throwException(e);
        }
        return true;
    }

    @Override
    public void shutdown() {
        if (!this.started.compareAndSet(true, false)) {
            return;
        }
        GrpcServerHelper.shutdownAndAwaitTermination(this.server);
    }

    @Override
    public void registerConnectionClosedEventListener(final ConnectionClosedEventListener listener) {
        // NO-OP
    }

    @SuppressWarnings("unchecked")
    @Override
    public void registerProcessor(final RpcProcessor processor) {
        final String interest = processor.interest();
        final Message reqIns = Requires.requireNonNull(this.parserClasses.get(interest), "null default instance: " + interest);
        final MethodDescriptor<Message, Message> method = MethodDescriptor //
                .<Message, Message>newBuilder() //
                .setType(MethodDescriptor.MethodType.UNARY) //
                .setFullMethodName(
                    MethodDescriptor.generateFullMethodName(processor.interest(), GrpcRaftRpcFactory.FIXED_METHOD_NAME)) //
                .setRequestMarshaller(ProtoUtils.marshaller(reqIns)) //
                .setResponseMarshaller(ProtoUtils.marshaller(MarshallerHelper.findRespInstance(interest))) //
                .build();

        final ServerCallHandler<Message, Message> handler = ServerCalls.asyncUnaryCall(
                (request, responseObserver) -> {
                    final RpcContext rpcCtx = new RpcContext() {

                        @Override
                        public void sendResponse(final Object responseObj) {
                            responseObserver.onNext((Message) responseObj);
                            responseObserver.onCompleted();
                        }

                        @Override
                        public Connection getConnection() {
                            throw new UnsupportedOperationException("unsupported");
                        }

                        @Override
                        public String getRemoteAddress() {
                            return null;
                        }
                    };

                    processor.handleRequest(rpcCtx, request);
                });

        final ServerServiceDefinition serviceDef = ServerServiceDefinition //
                .builder(processor.interest()) //
                .addMethod(method, handler) //
                .build();

        this.handlerRegistry.addService(serviceDef);
    }

    @Override
    public int boundPort() {
        return this.server.getPort();
    }
}
