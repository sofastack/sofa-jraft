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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.InvokeTimeoutException;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.util.DirectExecutor;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;

/**
 * GRPC RPC client implement.
 *
 * @author nicholas.jxf
 * @author jiachun.fjc
 */
public class GrpcClient implements RpcClient {

    private static final Logger                 LOG                = LoggerFactory.getLogger(GrpcClient.class);

    private final Map<Endpoint, ManagedChannel> managedChannelPool = new ConcurrentHashMap<>();
    private final Map<String, Message>          parserClasses;
    private final MarshallerRegistry            marshallerRegistry;
    private volatile ReplicatorGroup            replicatorGroup;

    public GrpcClient(Map<String, Message> parserClasses, MarshallerRegistry marshallerRegistry) {
        this.parserClasses = parserClasses;
        this.marshallerRegistry = marshallerRegistry;
    }

    @Override
    public boolean init(final RpcOptions opts) {
        // do nothing
        return true;
    }

    @Override
    public void shutdown() {
        closeAllChannels();
    }

    @Override
    public boolean checkConnection(final Endpoint endpoint) {
        Requires.requireNonNull(endpoint, "endpoint");
        return checkChannel(endpoint);
    }

    @Override
    public void closeConnection(final Endpoint endpoint) {
        Requires.requireNonNull(endpoint, "endpoint");
        closeChannel(endpoint);
    }

    @Override
    public void registerConnectEventListener(final ReplicatorGroup replicatorGroup) {
        this.replicatorGroup = replicatorGroup;
    }

    @Override
    public Object invokeSync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
                             final long timeoutMs) throws RemotingException {
        final CompletableFuture<Object> future = new CompletableFuture<>();

        invokeAsync(endpoint, request, ctx, (result, err) -> {
            if (err == null) {
                future.complete(result);
            } else {
                future.completeExceptionally(err);
            }
        }, timeoutMs);

        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            future.cancel(true);
            throw new InvokeTimeoutException(e);
        } catch (final Throwable t) {
            future.cancel(true);
            throw new RemotingException(t);
        }
    }

    @Override
    public void invokeAsync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
                            final InvokeCallback callback, final long timeoutMs) {
        Requires.requireNonNull(endpoint, "endpoint");
        Requires.requireNonNull(request, "request");

        final Channel ch = getChannel(endpoint);
        final MethodDescriptor<Message, Message> method = getCallMethod(request);
        final CallOptions callOpts = CallOptions.DEFAULT.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
        final Executor executor = callback.executor() != null ? callback.executor() : DirectExecutor.INSTANCE;

        ClientCalls.asyncUnaryCall(ch.newCall(method, callOpts), (Message) request, new StreamObserver<Message>() {

            @Override
            public void onNext(final Message value) {
                executor.execute(() -> callback.complete(value, null));
            }

            @Override
            public void onError(final Throwable throwable) {
                executor.execute(() -> callback.complete(null, throwable));
            }

            @Override
            public void onCompleted() {
                // NO-OP
            }
        });
    }

    private MethodDescriptor<Message, Message> getCallMethod(final Object request) {
        final String interest = request.getClass().getName();
        final Message reqIns = Requires.requireNonNull(this.parserClasses.get(interest), "null default instance: "
                                                                                         + interest);
        return MethodDescriptor //
            .<Message, Message> newBuilder() //
            .setType(MethodDescriptor.MethodType.UNARY) //
            .setFullMethodName(MethodDescriptor.generateFullMethodName(interest, GrpcRaftRpcFactory.FIXED_METHOD_NAME)) //
            .setRequestMarshaller(ProtoUtils.marshaller(reqIns)) //
            .setResponseMarshaller(
                ProtoUtils.marshaller(this.marshallerRegistry.findResponseInstanceByRequest(interest))) //
            .build();
    }

    private Channel getChannel(final Endpoint endpoint) {
        return this.managedChannelPool.computeIfAbsent(endpoint, ep -> {
            final ManagedChannel ch = ManagedChannelBuilder.forAddress(ep.getIp(), ep.getPort()) //
                .usePlaintext() //
                .directExecutor() //
                .build();
            // channel connection event
            ch.notifyWhenStateChanged(ConnectivityState.READY, () -> {
                final ReplicatorGroup rpGroup = replicatorGroup;
                if (rpGroup != null) {
                    Utils.runInThread(() -> {
                        final PeerId peer = new PeerId();
                        if (peer.parse(ep.toString())) {
                            LOG.info("Peer {} is connected.", peer);
                            rpGroup.checkReplicator(peer, true);
                        } else {
                            LOG.error("Fail to parse peer: {}.", ep);
                        }
                    });
                }
            });

            return ch;
        });
    }

    private void closeAllChannels() {
        for (final Map.Entry<Endpoint, ManagedChannel> entry : this.managedChannelPool.entrySet()) {
            final ManagedChannel ch = entry.getValue();
            LOG.info("Shutdown managed channel: {}, {}.", entry.getKey(), ch);
            ManagedChannelHelper.shutdownAndAwaitTermination(ch);
        }
    }

    private void closeChannel(final Endpoint endpoint) {
        final ManagedChannel ch = this.managedChannelPool.remove(endpoint);
        LOG.info("Close connection: {}, {}.", endpoint, ch);
        if (ch != null) {
            ManagedChannelHelper.shutdownAndAwaitTermination(ch);
        }
    }

    private boolean checkChannel(final Endpoint endpoint) {
        final ManagedChannel ch = this.managedChannelPool.get(endpoint);
        if (ch == null) {
            return false;
        }
        final ConnectivityState st = ch.getState(true);
        return st == ConnectivityState.CONNECTING || st == ConnectivityState.READY || st == ConnectivityState.IDLE;
    }
}
