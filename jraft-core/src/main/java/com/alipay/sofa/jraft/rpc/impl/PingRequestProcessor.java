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

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcRequests.PingRequest;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

import java.util.concurrent.Executor;

/**
 * Ping request processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * @author jiachun.fjc
 */
public class PingRequestProcessor implements RpcProcessor<PingRequest> {

    private final Executor executor;

    public PingRequestProcessor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final PingRequest request) {
        rpcCtx.sendResponse( //
            RpcFactoryHelper //
                .responseFactory() //
                .newResponse(RpcRequests.ErrorResponse.getDefaultInstance(), 0, "OK"));
    }

    @Override
    public String interest() {
        return PingRequest.class.getName();
    }

    @Override
    public Executor executor() {
        return this.executor;
    }
}
