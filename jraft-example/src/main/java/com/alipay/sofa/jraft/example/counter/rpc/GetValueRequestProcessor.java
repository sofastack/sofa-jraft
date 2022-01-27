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
package com.alipay.sofa.jraft.example.counter.rpc;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.example.counter.CounterClosure;
import com.alipay.sofa.jraft.example.counter.CounterService;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.example.counter.rpc.CounterOutter.GetValueRequest;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

/**
 * GetValueRequest processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 5:48:33 PM
 */
public class GetValueRequestProcessor implements RpcProcessor<GetValueRequest> {

    private final CounterService counterService;

    public GetValueRequestProcessor(CounterService counterService) {
        super();
        this.counterService = counterService;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final GetValueRequest request) {
        final CounterClosure closure = new CounterClosure() {
            @Override
            public void run(Status status) {
                rpcCtx.sendResponse(getValueResponse());
            }
        };

        this.counterService.get(request.getReadOnlySafe(), closure);
    }

    @Override
    public String interest() {
        return GetValueRequest.class.getName();
    }
}
