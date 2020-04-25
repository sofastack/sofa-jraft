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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.grpc.CallOptions.DEFAULT;
import static io.grpc.MethodDescriptor.MethodType.UNARY;
import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * GRPC RPC client implement.
 *
 * @author nicholas.jxf
 */
public class GrpcClient implements RpcClient {

    private volatile Map<Endpoint, ManagedChannel> managedChannels = new ConcurrentHashMap<>();

    @Override
    public boolean init(final RpcOptions opts) {
        return true;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean checkConnection(final Endpoint endpoint) {
        Requires.requireNonNull(endpoint, "endpoint");
        return true;
    }

    @Override
    public void closeConnection(final Endpoint endpoint) {
        ManagedChannel channel = managedChannels.remove(endpoint);
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    @Override
    public void registerConnectEventListener(final ReplicatorGroup replicatorGroup) {
    }

    @Override
    public Object invokeSync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
                             final long timeoutMs) {
        Requires.requireNonNull(endpoint, "endpoint");
        ManagedChannel channel;
        if (managedChannels.containsKey(endpoint)) {
            channel = managedChannels.get(endpoint);
        } else {
            channel = ManagedChannelBuilder.forTarget(endpoint.toString()).usePlaintext().build();
            managedChannels.put(endpoint, channel);
        }
        MethodDescriptor<Object, Object> method = MethodDescriptor.newBuilder().setType(UNARY)
            .setFullMethodName(generateFullMethodName(RpcProcessor.class.getName(), request.getClass().getName()))
            .build();
        return ClientCalls.blockingUnaryCall(channel, method, DEFAULT, request);
    }

    @Override
    public void invokeAsync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
                            final InvokeCallback callback, final long timeoutMs) {
        Requires.requireNonNull(endpoint, "endpoint");
        ManagedChannel channel;
        if (managedChannels.containsKey(endpoint)) {
            channel = managedChannels.get(endpoint);
        } else {
            channel = ManagedChannelBuilder.forTarget(endpoint.toString()).usePlaintext().build();
            managedChannels.put(endpoint, channel);
        }
        MethodDescriptor<Object, Object> method = MethodDescriptor
            .newBuilder()
            .setType(UNARY)
            .setFullMethodName(
                generateFullMethodName(RpcProcessor.class.getSimpleName(), request.getClass().getSimpleName())) //
            .setRequestMarshaller(ObjectMarshaller.INSTANCE) //
            .setResponseMarshaller(ObjectMarshaller.INSTANCE) //
            .build();
        ClientCalls.asyncUnaryCall(channel.newCall(method, DEFAULT), request, new StreamObserver<Object>() {
            @Override
            public void onNext(Object o) {
                callback.complete(o, null);
            }

            @Override
            public void onError(Throwable throwable) {
                callback.complete(null, throwable);
            }

            @Override
            public void onCompleted() {
            }
        });
    }
}
