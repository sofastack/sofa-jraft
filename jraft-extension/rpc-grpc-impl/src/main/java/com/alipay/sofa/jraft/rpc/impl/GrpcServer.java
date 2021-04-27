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
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.util.MutableHandlerRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.google.protobuf.Message;

/**
 * GRPC RPC server implement.
 *
 * @author nicholas.jxf
 * @author jiachun.fjc
 */
public class GrpcServer implements RpcServer {

    private static final Logger                       LOG                  = LoggerFactory.getLogger(GrpcServer.class);

    private static final String                       EXECUTOR_NAME        = "grpc-default-executor";

    private final Server                              server;
    private final MutableHandlerRegistry              handlerRegistry;
    private final Map<String, Message>                parserClasses;
    private final MarshallerRegistry                  marshallerRegistry;
    private final List<ServerInterceptor>             serverInterceptors   = new CopyOnWriteArrayList<>();
    private final List<ConnectionClosedEventListener> closedEventListeners = new CopyOnWriteArrayList<>();
    private final AtomicBoolean                       started              = new AtomicBoolean(false);

    private ExecutorService                           defaultExecutor;

    public GrpcServer(Server server, MutableHandlerRegistry handlerRegistry, Map<String, Message> parserClasses,
                      MarshallerRegistry marshallerRegistry) {
        this.server = server;
        this.handlerRegistry = handlerRegistry;
        this.parserClasses = parserClasses;
        this.marshallerRegistry = marshallerRegistry;
        registerDefaultServerInterceptor();
    }

    @Override
    public boolean init(final Void opts) {
        if (!this.started.compareAndSet(false, true)) {
            throw new IllegalStateException("grpc server has started");
        }

        this.defaultExecutor = ThreadPoolUtil.newBuilder() //
            .poolName(EXECUTOR_NAME) //
            .enableMetric(true) //
            .coreThreads(Math.min(20, GrpcRaftRpcFactory.RPC_SERVER_PROCESSOR_POOL_SIZE / 5)) //
            .maximumThreads(GrpcRaftRpcFactory.RPC_SERVER_PROCESSOR_POOL_SIZE) //
            .keepAliveSeconds(60L) //
            .workQueue(new SynchronousQueue<>()) //
            .threadFactory(new NamedThreadFactory(EXECUTOR_NAME + "-", true)) //
            .rejectedHandler((r, executor) -> {
                throw new RejectedExecutionException("[" + EXECUTOR_NAME + "], task " + r.toString() +
                        " rejected from " +
                        executor.toString());
            })
            .build();

        try {
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
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.defaultExecutor);
        GrpcServerHelper.shutdownAndAwaitTermination(this.server);
    }

    @Override
    public void registerConnectionClosedEventListener(final ConnectionClosedEventListener listener) {
        this.closedEventListeners.add(listener);
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
                .setResponseMarshaller(ProtoUtils.marshaller(this.marshallerRegistry.findResponseInstanceByRequest(interest))) //
                .build();

        final ServerCallHandler<Message, Message> handler = ServerCalls.asyncUnaryCall(
                (request, responseObserver) -> {
                    final SocketAddress remoteAddress = RemoteAddressInterceptor.getRemoteAddress();
                    final Connection conn = ConnectionInterceptor.getCurrentConnection(this.closedEventListeners);

                    final RpcContext rpcCtx = new RpcContext() {

                        @Override
                        public void sendResponse(final Object responseObj) {
                            try {
                                responseObserver.onNext((Message) responseObj);
                                responseObserver.onCompleted();
                            } catch (final Throwable t) {
                                LOG.warn("[GRPC] failed to send response.", t);
                            }
                        }

                        @Override
                        public Connection getConnection() {
                            if (conn == null) {
                                throw new IllegalStateException("fail to get connection");
                            }
                            return conn;
                        }

                        @Override
                        public String getRemoteAddress() {
                            // Rely on GRPC's capabilities, not magic (netty channel)
                            return remoteAddress != null ? remoteAddress.toString() : null;
                        }
                    };

                    final RpcProcessor.ExecutorSelector selector = processor.executorSelector();
                    Executor executor;
                    if (selector != null && request instanceof RpcRequests.AppendEntriesRequest) {
                        final RpcRequests.AppendEntriesRequest req = (RpcRequests.AppendEntriesRequest) request;
                        final RpcRequests.AppendEntriesRequestHeader.Builder header = RpcRequests.AppendEntriesRequestHeader //
                                .newBuilder() //
                                .setGroupId(req.getGroupId()) //
                                .setPeerId(req.getPeerId()) //
                                .setServerId(req.getServerId());
                        executor = selector.select(interest, header.build());
                    } else {
                        executor = processor.executor();
                    }

                    if (executor == null) {
                        executor = this.defaultExecutor;
                    }

                    if (executor != null) {
                        executor.execute(() -> processor.handleRequest(rpcCtx, request));
                    } else {
                        processor.handleRequest(rpcCtx, request);
                    }
                });

        final ServerServiceDefinition serviceDef = ServerServiceDefinition //
                .builder(interest) //
                .addMethod(method, handler) //
                .build();

        this.handlerRegistry
            .addService(ServerInterceptors.intercept(serviceDef, this.serverInterceptors.toArray(new ServerInterceptor[0])));
    }

    @Override
    public int boundPort() {
        return this.server.getPort();
    }

    public void setDefaultExecutor(ExecutorService defaultExecutor) {
        this.defaultExecutor = defaultExecutor;
    }

    public Server getServer() {
        return server;
    }

    public MutableHandlerRegistry getHandlerRegistry() {
        return handlerRegistry;
    }

    public boolean addServerInterceptor(final ServerInterceptor interceptor) {
        return this.serverInterceptors.add(interceptor);
    }

    private void registerDefaultServerInterceptor() {
        this.serverInterceptors.add(new RemoteAddressInterceptor());
        this.serverInterceptors.add(new ConnectionInterceptor());
    }
}
