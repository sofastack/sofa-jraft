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
package com.alipay.sofa.jraft.rpc;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;

/**
 * RPC request Closure encapsulates the RPC contexts.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * @author jiachun.fjc
 */
public class RpcRequestClosure implements Closure {

    private static final Logger                                       LOG           = LoggerFactory
                                                                                        .getLogger(RpcRequestClosure.class);

    private static final AtomicIntegerFieldUpdater<RpcRequestClosure> STATE_UPDATER = AtomicIntegerFieldUpdater
                                                                                        .newUpdater(
                                                                                            RpcRequestClosure.class,
                                                                                            "state");

    private static final int                                          PENDING       = 0;
    private static final int                                          RESPOND       = 1;

    private final RpcContext                                          rpcCtx;
    private final Message                                             defaultResp;

    private volatile int                                              state         = PENDING;

    public RpcRequestClosure(RpcContext rpcCtx) {
        this(rpcCtx, null);
    }

    public RpcRequestClosure(RpcContext rpcCtx, Message defaultResp) {
        super();
        this.rpcCtx = rpcCtx;
        this.defaultResp = defaultResp;
    }

    public RpcContext getRpcCtx() {
        return rpcCtx;
    }

    public void sendResponse(final Message msg) {
        if (!STATE_UPDATER.compareAndSet(this, PENDING, RESPOND)) {
            LOG.warn("A response: {} sent repeatedly!", msg);
            return;
        }
        this.rpcCtx.sendResponse(msg);
    }

    @Override
    public void run(final Status status) {
        sendResponse(RpcFactoryHelper.responseFactory().newResponse(this.defaultResp, status));
    }
}
