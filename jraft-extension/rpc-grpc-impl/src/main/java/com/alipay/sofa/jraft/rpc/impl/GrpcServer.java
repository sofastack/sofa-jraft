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

import com.alipay.sofa.jraft.util.Endpoint;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.util.MutableHandlerRegistry;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;

import static io.grpc.MethodDescriptor.MethodType.UNARY;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

/**
 * GRPC RPC server implement.
 *
 * @author nicholas.jxf
 */
public class GrpcServer implements RpcServer {
    private final Endpoint                  endpoint;
    private Server                          server;
    private ServerServiceDefinition.Builder serviceDefinitionBuilder;
    private final AtomicBoolean             started = new AtomicBoolean(false);

    public GrpcServer(Endpoint endpoint) {
        this.endpoint = endpoint;
        this.serviceDefinitionBuilder = ServerServiceDefinition.builder(RpcProcessor.class.getSimpleName());
    }

    @Override
    public boolean init(final Void opts) {
        if (!this.started.compareAndSet(false, true)) {
            throw new IllegalStateException("grpc server has started");
        }
        try {
            MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry();
            handlerRegistry.addService(this.serviceDefinitionBuilder.build());
            this.server = NettyServerBuilder.forAddress(this.endpoint.toInetSocketAddress())
                .fallbackHandlerRegistry(handlerRegistry).build();
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
        this.server.shutdown();
    }

    @Override
    public void registerConnectionClosedEventListener(final ConnectionClosedEventListener listener) {
    }

    @Override
    public void registerProcessor(final RpcProcessor processor) {
        MethodDescriptor<Object, Object> method = MethodDescriptor.newBuilder() //
                .setType(UNARY) //
                .setFullMethodName(generateFullMethodName(RpcProcessor.class.getSimpleName(),
                        processor.interest().split("\\$")[1])) //
                .setRequestMarshaller(ObjectMarshaller.INSTANCE) //
                .setResponseMarshaller(ObjectMarshaller.INSTANCE) //
                .build();
        this.serviceDefinitionBuilder.addMethod(method, asyncUnaryCall(
                (request, responseObserver) -> {
                    final RpcContext rpcCtx = new RpcContext() {
                        
                        @Override
                        public void sendResponse(final Object responseObj) {
                            responseObserver.onNext(responseObj);
                            responseObserver.onCompleted();
                        }
                        
                        @Override
                        public Connection getConnection() {
                            return null;
                        }
                        
                        @Override
                        public String getRemoteAddress() {
                            return null;
                        }
                    };
                    processor.handleRequest(rpcCtx, request);
                }));
    }

    @Override
    public int boundPort() {
        return this.server.getPort();
    }
}
