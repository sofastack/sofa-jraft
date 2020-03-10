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

import java.util.concurrent.Executor;

import com.alipay.remoting.Url;
import com.alipay.remoting.config.switches.GlobalSwitch;
import com.alipay.remoting.rpc.RpcAddressParser;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;

/**
 * Bolt rpc client impl
 *
 * @author jiachun.fjc
 */
public class BoltRpcClient implements RpcClient {

    public static final String                      BOLT_ADDRESS_PARSER = "BOLT_ADDRESS_PARSER";
    public static final String                      BOLT_CTX            = "BOLT_CTX";

    private final com.alipay.remoting.rpc.RpcClient rpcClient;

    public BoltRpcClient(com.alipay.remoting.rpc.RpcClient rpcClient) {
        this.rpcClient = Requires.requireNonNull(rpcClient, "rpcClient");
    }

    @Override
    public boolean init(final Void opts) {
        this.rpcClient.switches().turnOn(GlobalSwitch.CODEC_FLUSH_CONSOLIDATION);
        this.rpcClient.startup();
        return true;
    }

    @Override
    public void shutdown() {
        this.rpcClient.shutdown();
    }

    @Override
    public void invokeSync(final Endpoint endpoint, final Object request, final InvokeContext ctx, final long timeoutMs) {

    }

    @Override
    public void invokeAsync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
                            final InvokeCallback callback, final long timeoutMs) {
        Requires.requireNonNull(endpoint, "endpoint");
        final RpcAddressParser addressParser = getAddressParser(ctx);
        try {
            if (addressParser != null) {
                final Url url = addressParser.parse(endpoint.toString());
                this.rpcClient.invokeWithCallback(url, request, getBoltInvokeCtx(ctx), getBoltCallback(callback),
                    (int) timeoutMs);
            } else {
                this.rpcClient.invokeWithCallback(endpoint.toString(), request, getBoltInvokeCtx(ctx),
                    getBoltCallback(callback), (int) timeoutMs);
            }
        } catch (final Throwable t) {
            ThrowUtil.throwException(t);
        }
    }

    private RpcAddressParser getAddressParser(final InvokeContext ctx) {
        return ctx == null ? null : ctx.get(BOLT_ADDRESS_PARSER);
    }

    private com.alipay.remoting.InvokeContext getBoltInvokeCtx(final InvokeContext ctx) {
        return ctx == null ? null : ctx.get(BOLT_CTX);
    }

    private BoltCallback getBoltCallback(final InvokeCallback callback) {
        return new BoltCallback(callback);
    }

    private static class BoltCallback implements com.alipay.remoting.InvokeCallback {

        private final InvokeCallback callback;

        private BoltCallback(InvokeCallback callback) {
            this.callback = callback;
        }

        @Override
        public void onResponse(final Object result) {
            this.callback.complete(result, null);
        }

        @Override
        public void onException(final Throwable err) {
            this.callback.complete(null, err);
        }

        @Override
        public Executor getExecutor() {
            return this.callback.executor();
        }
    }
}
