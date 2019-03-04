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

import java.util.concurrent.Future;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;

/**
 * RPC client service
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 3:21:56 PM
 */
public interface ClientService extends Lifecycle<RpcOptions> {

    /**
     * Connect to endpoint, returns true when success.
     *
     * @param endpoint server address
     * @return true on connect success
     */
    boolean connect(Endpoint endpoint);

    /**
     * Disconnect from endpoint.
     *
     * @param endpoint server address
     * @return true on disconnect success
     */
    boolean disconnect(Endpoint endpoint);

    /**
     * Returns true when the endpoint's connection is active.
     *
     * @param endpoint server address
     * @return true on connection is active
     */
    boolean isConnected(Endpoint endpoint);

    /**
     * Send a requests and waits for response with callback, returns the request future.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @param timeoutMs timeout millis
     * @return a future with operation result
     */
    <T extends Message> Future<Message> invokeWithDone(Endpoint endpoint, Message request, RpcResponseClosure<T> done,
                                                       int timeoutMs);
}
