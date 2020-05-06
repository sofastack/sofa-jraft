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
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.google.protobuf.Message;

/**
 * GRPC RPC client implement.
 *
 * @author nicholas.jxf
 * @author jiachun.fjc
 */
public class GrpcClient implements RpcClient {

    private static final Logger                 LOG             = LoggerFactory.getLogger(GrpcClient.class);

    private final Map<Endpoint, ManagedChannel> managedChannels = new ConcurrentHashMap<>();
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
        for (final Map.Entry<Endpoint, ManagedChannel> entry : this.managedChannels.entrySet()) {
            final ManagedChannel ch = entry.getValue();
            LOG.info("Shutdown managed channel: {}, {}.", entry.getKey(), ch);
            ManagedChannelHelper.shutdownAndAwaitTermination(ch);
        }
    }

    @Override
    public boolean checkConnection(final Endpoint endpoint) {
        Requires.requireNonNull(endpoint, "endpoint");
        final ManagedChannel ch = this.managedChannels.get(endpoint);
        if (ch == null) {
            return false;
        }
        final ConnectivityState st = ch.getState(true);
        return st == ConnectivityState.CONNECTING || st == ConnectivityState.READY || st == ConnectivityState.IDLE;
    }

    @Override
    public void closeConnection(final Endpoint endpoint) {
        Requires.requireNonNull(endpoint, "endpoint");
        final ManagedChannel ch = this.managedChannels.remove(endpoint);
        LOG.info("Close connection: {}, {}.", endpoint, ch);
        if (ch != null) {
            ManagedChannelHelper.shutdownAndAwaitTermination(ch);
        }
    }

    @Override
    public void registerConnectEventListener(final ReplicatorGroup replicatorGroup) {
        this.replicatorGroup = replicatorGroup;
    }

    @Override
    public Object invokeSync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
                             final long timeoutMs) throws RemotingException {
        Requires.requireNonNull(endpoint, "endpoint");
        Requires.requireNonNull(request, "request");
        final Channel ch = getChannel(endpoint);
        final MethodDescriptor<Message, Message> method = getCallMethod(request);
        try {
            return ClientCalls.blockingUnaryCall(ch, method, CallOptions.DEFAULT, (Message) request);
        } catch (final Throwable t) {
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
        ClientCalls.asyncUnaryCall(ch.newCall(method, CallOptions.DEFAULT), (Message) request,
            new StreamObserver<Message>() {

                @Override
                public void onNext(final Message value) {
                    callback.complete(value, null);
                }

                @Override
                public void onError(final Throwable throwable) {
                    callback.complete(null, throwable);
                }

                @Override
                public void onCompleted() {
                    // TODO ?
                }
            });
    }

    private MethodDescriptor<Message, Message> getCallMethod(final Object request) {
        final String interest = request.getClass().getName();
        final Message reqIns = Requires.requireNonNull(this.parserClasses.get(interest), "null default instance: "
                                                                                         + interest);
        return MethodDescriptor //
            .<Message, Message> newBuilder() //
            .setType(MethodDescriptor.MethodType.UNARY)
            //
            .setFullMethodName(MethodDescriptor.generateFullMethodName(interest, GrpcRaftRpcFactory.FIXED_METHOD_NAME)) //
            .setRequestMarshaller(ProtoUtils.marshaller(reqIns)) //
            .setResponseMarshaller(
                ProtoUtils.marshaller(this.marshallerRegistry.findResponseInstanceByRequest(interest))) //
            .build();
    }

    private Channel getChannel(final Endpoint endpoint) {
        ManagedChannel ch = this.managedChannels.get(endpoint);
        if (ch == null) {
            final ManagedChannel newCh = ManagedChannelBuilder.forTarget(endpoint.toString()) //
                    .usePlaintext() //
                    .build();
            ch = this.managedChannels.putIfAbsent(endpoint, newCh);
            if (ch == null) {
                ch = newCh;
                // channel connection event
                ch.notifyWhenStateChanged(ConnectivityState.READY, () -> {
                    final ReplicatorGroup rpGroup = replicatorGroup;
                    if (rpGroup != null) {
                        final PeerId peer = new PeerId();
                        if (peer.parse(endpoint.toString())) {
                            LOG.info("Peer {} is connected.", peer);
                            rpGroup.checkReplicator(peer, true);
                        } else {
                            LOG.error("Fail to parse peer: {}.", endpoint);
                        }
                    }
                });
            } else {
                ManagedChannelHelper.shutdownAndAwaitTermination(newCh, 100);
            }
        }

        return ch;
    }
}
