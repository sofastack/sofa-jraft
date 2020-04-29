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
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.internal.ServerCallImpl;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCalls;
import io.grpc.util.MutableHandlerRegistry;

import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;

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

    private Server                       server;

    public GrpcServer(Endpoint endpoint) {
        this.endpoint = endpoint;
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
        // TODO
    }

    @SuppressWarnings("unchecked")
    @Override
    public void registerProcessor(final RpcProcessor processor) {
        final MethodDescriptor<Object, Object> method = MethodDescriptor.newBuilder() //
                .setType(MethodDescriptor.MethodType.UNARY) //
                .setFullMethodName(
                    MethodDescriptor.generateFullMethodName(processor.interest(), GrpcRaftRpcFactory.FIXED_METHOD_NAME)) //
                .setRequestMarshaller(ObjectMarshaller.INSTANCE) //
                .setResponseMarshaller(ObjectMarshaller.INSTANCE) //
                .build();

        final ServerCallHandler<Object, Object> handler = ServerCalls.asyncUnaryCall(
                (request, responseObserver) -> {

                    final ServerCallImpl<Object, Object> call = (ServerCallImpl<Object, Object>) ((ServerCalls.ServerCallStreamObserverImpl) responseObserver) //
                        .getCall();

                    final RpcContext rpcCtx = new RpcContext() {

                        @Override
                        public void sendResponse(final Object responseObj) {
                            responseObserver.onNext(responseObj);
                            responseObserver.onCompleted();
                        }

                        @Override
                        public Connection getConnection() {
                            return new GrpcConnection(call);
                        }

                        @Override
                        public String getRemoteAddress() {
                            return null; // TODO
                        }
                    };

                    processor.handleRequest(rpcCtx, request);
                });

        final ServerServiceDefinition serverServiceDefinition = ServerServiceDefinition //
                .builder(processor.interest()) //
                .addMethod(method, handler) //
                .build();
        this.handlerRegistry.addService(serverServiceDefinition);
    }

    @Override
    public int boundPort() {
        return this.server.getPort();
    }

    private class GrpcConnection implements Connection {

        @SuppressWarnings("unused")
        private final ServerCallImpl<Object, Object> call;

        private GrpcConnection(ServerCallImpl<Object, Object> call) {
            this.call = call;
        }

        @Override
        public Object getAttribute(final String key) {
            return this.call.getAttributes().get(Attributes.Key.create(key));
        }

        @Override
        public void setAttribute(final String key, final Object value) {
            this.call.getAttributes().toBuilder().set(Attributes.Key.create(key), value).build();
        }

        @Override
        public void close() {
            this.call.close(Status.ABORTED, new Metadata());
        }
    }
}
