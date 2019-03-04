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
package com.alipay.sofa.jraft.rhea.client;

import java.util.concurrent.CompletableFuture;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.client.failover.FailoverClosure;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;

/**
 * RheaKV's rpc client for sending kv requests and receiving kv responses.
 *
 * @author jiachun.fjc
 */
public interface RheaKVRpcService extends Lifecycle<RpcOptions> {

    /**
     * @see #callAsyncWithRpc(BaseRequest, FailoverClosure, Errors, boolean)
     */
    <V> CompletableFuture<V> callAsyncWithRpc(final BaseRequest request, final FailoverClosure<V> closure,
                                              final Errors lastCause);

    /**
     * Send KV requests to the remote data service nodes.
     *
     * @param request       request data
     * @param closure       callback for failover strategy
     * @param lastCause     the exception information held by the last call
     *                      failed, the initial value is null
     * @param requireLeader if true, then request to call the leader node
     * @param <V>           the type of response
     * @return a future with response
     */
    <V> CompletableFuture<V> callAsyncWithRpc(final BaseRequest request, final FailoverClosure<V> closure,
                                              final Errors lastCause, final boolean requireLeader);
}
