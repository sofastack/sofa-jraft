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
package com.alipay.sofa.jraft.rpc.impl.core;

import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.google.protobuf.Message;

/**
 * TimeoutNow request processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-23 11:12:48 AM
 */
public class TimeoutNowRequestProcessor extends NodeRequestProcessor<TimeoutNowRequest> {

    public TimeoutNowRequestProcessor(Executor executor) {
        super(executor);
    }

    @Override
    protected String getPeerId(TimeoutNowRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(TimeoutNowRequest request) {
        return request.getGroupId();
    }

    @Override
    public Message processRequest0(RaftServerService service, TimeoutNowRequest request, RpcRequestClosure done) {
        return service.handleTimeoutNowRequest(request, done);
    }

    @Override
    public String interest() {
        return TimeoutNowRequest.class.getName();
    }
}