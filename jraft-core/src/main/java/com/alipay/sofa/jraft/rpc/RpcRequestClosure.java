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

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.google.protobuf.Message;

/**
 * RPC request Closure encapsulates the RPC contexts.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-28 4:55:24 PM
 */
public class RpcRequestClosure implements Closure {

    private final BizContext   bizContext;
    private final AsyncContext asyncContext;
    private boolean            respond;

    public RpcRequestClosure(BizContext bizContext, AsyncContext asyncContext) {
        super();
        this.bizContext = bizContext;
        this.asyncContext = asyncContext;
        this.respond = false;
    }

    public BizContext getBizContext() {
        return this.bizContext;
    }

    public AsyncContext getAsyncContext() {
        return this.asyncContext;
    }

    public synchronized void sendResponse(Message msg) {
        if (this.respond) {
            return;
        }
        this.asyncContext.sendResponse(msg);
        this.respond = true;
    }

    @Override
    public void run(Status status) {
        sendResponse(RpcResponseFactory.newResponse(status));
    }
}
