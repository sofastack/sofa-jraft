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

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.config.switches.GlobalSwitch;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Bolt RPC server impl.
 *
 * @author jiachun.fjc
 */
public class BoltRpcServer implements RpcServer {

    private final com.alipay.remoting.rpc.RpcServer rpcServer;

    public BoltRpcServer(com.alipay.remoting.rpc.RpcServer rpcServer) {
        this.rpcServer = Requires.requireNonNull(rpcServer, "rpcServer");
    }

    @Override
    public boolean init(final Void opts) {
        this.rpcServer.switches().turnOn(GlobalSwitch.CODEC_FLUSH_CONSOLIDATION);
        this.rpcServer.startup();
        return true;
    }

    @Override
    public void shutdown() {
        this.rpcServer.shutdown();
    }

    @Override
    public void registerProcessor(final RpcProcessor<?> processor) {
        this.rpcServer.registerUserProcessor(new AsyncUserProcessor<Object>() {

            @Override
            public void handleRequest(final BizContext bizCtx, final AsyncContext asyncCtx, final Object request) {
                processor.handleRequest(asyncCtx::sendResponse, request);
            }

            @Override
            public String interest() {
                return processor.interest();
            }
        });
    }
}
