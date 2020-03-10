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

import com.alipay.sofa.jraft.util.Endpoint;

/**
 * Raft RPC service factory.
 *
 * @author jiachun.fjc
 */
public interface RaftRpcFactory {

    /**
     * Creates a raft RPC client.
     *
     * @return a new rpc client instance
     */
    default RpcClient newRpcClient() {
        return newRpcClient(null);
    }

    /**
     * Creates a raft RPC client.
     *
     * @param helper config helper for rpc client impl
     * @return a new rpc client instance
     */
    RpcClient newRpcClient(final ConfigHelper<RpcClient> helper);

    /**
     * Creates a raft RPC server.
     *
     * @param endpoint server address to bind
     * @return a new rpc server instance
     */
    default RpcServer newRpcServer(final Endpoint endpoint) {
        return newRpcServer(endpoint, null);
    }

    /**
     * Creates a raft RPC server.
     *
     * @param endpoint server address to bind
     * @param helper   config helper for rpc server impl
     * @return a new rpc server instance
     */
    RpcServer newRpcServer(final Endpoint endpoint, final ConfigHelper<RpcServer> helper);

    interface ConfigHelper<T> {

        void config(final T instance);
    }
}
