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
package com.alipay.sofa.jraft.rhea.options;

import com.alipay.sofa.jraft.util.Utils;

/**
 *
 * @author jiachun.fjc
 */
public class RpcOptions {

    private int callbackExecutorCorePoolSize    = Utils.cpus() << 2;
    private int callbackExecutorMaximumPoolSize = Utils.cpus() << 3;
    private int callbackExecutorQueueCapacity   = 512;
    private int rpcTimeoutMillis                = 5000;

    public int getCallbackExecutorCorePoolSize() {
        return callbackExecutorCorePoolSize;
    }

    public void setCallbackExecutorCorePoolSize(int callbackExecutorCorePoolSize) {
        this.callbackExecutorCorePoolSize = callbackExecutorCorePoolSize;
    }

    public int getCallbackExecutorMaximumPoolSize() {
        return callbackExecutorMaximumPoolSize;
    }

    public void setCallbackExecutorMaximumPoolSize(int callbackExecutorMaximumPoolSize) {
        this.callbackExecutorMaximumPoolSize = callbackExecutorMaximumPoolSize;
    }

    public int getCallbackExecutorQueueCapacity() {
        return callbackExecutorQueueCapacity;
    }

    public void setCallbackExecutorQueueCapacity(int callbackExecutorQueueCapacity) {
        this.callbackExecutorQueueCapacity = callbackExecutorQueueCapacity;
    }

    public int getRpcTimeoutMillis() {
        return rpcTimeoutMillis;
    }

    public void setRpcTimeoutMillis(int rpcTimeoutMillis) {
        this.rpcTimeoutMillis = rpcTimeoutMillis;
    }

    @Override
    public String toString() {
        return "RpcOptions{" + "callbackExecutorCorePoolSize=" + callbackExecutorCorePoolSize
               + ", callbackExecutorMaximumPoolSize=" + callbackExecutorMaximumPoolSize
               + ", callbackExecutorQueueCapacity=" + callbackExecutorQueueCapacity + ", rpcTimeoutMillis="
               + rpcTimeoutMillis + '}';
    }
}
