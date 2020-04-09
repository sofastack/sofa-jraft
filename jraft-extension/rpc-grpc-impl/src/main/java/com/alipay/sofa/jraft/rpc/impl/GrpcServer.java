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
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.Server;
import io.grpc.util.MutableHandlerRegistry;

import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;

/**
 * @author jiachun.fjc
 */
public class GrpcServer implements RpcServer {

    private final Server                 server;
    private final MutableHandlerRegistry handlerRegistry;
    private final AtomicBoolean          start = new AtomicBoolean(false);

    public GrpcServer(Server server, MutableHandlerRegistry handlerRegistry) {
        this.server = server;
        this.handlerRegistry = handlerRegistry;
    }

    @Override
    public boolean init(final Void opts) {
        if (!this.start.compareAndSet(false, true)) {
            throw new IllegalStateException("grpc server has started");
        }

        try {
            this.server.start();
        } catch (final IOException e) {
            ThrowUtil.throwException(e);
        }
        return true;
    }

    @Override
    public void shutdown() {
        if (!this.start.compareAndSet(true, false)) {
            return;
        }
        this.server.shutdown();
    }

    @Override
    public void registerConnectionClosedEventListener(final ConnectionClosedEventListener listener) {

    }

    @Override
    public void registerProcessor(final RpcProcessor processor) {
        // final ServerServiceDefinition definition = ServerServiceDefinition.builder(processor.interest()).addMethod()
        //    .build();
    }

    @Override
    public int boundPort() {
        return this.server.getPort();
    }
}
