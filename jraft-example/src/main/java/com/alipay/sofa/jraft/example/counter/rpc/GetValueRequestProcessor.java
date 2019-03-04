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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.SyncUserProcessor;
import com.alipay.sofa.jraft.example.counter.CounterServer;

/**
 * GetValueRequest processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 5:48:33 PM
 */
public class GetValueRequestProcessor extends SyncUserProcessor<GetValueRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(GetValueRequestProcessor.class);

    private CounterServer       counterServer;

    public GetValueRequestProcessor(CounterServer counterServer) {
        super();
        this.counterServer = counterServer;
    }

    @Override
    public Object handleRequest(final BizContext bizCtx, final GetValueRequest request) throws Exception {
        if (!this.counterServer.getFsm().isLeader()) {
            return this.counterServer.redirect();
        }

        final ValueResponse response = new ValueResponse();
        response.setSuccess(true);
        response.setValue(this.counterServer.getFsm().getValue());
        return response;
    }

    @Override
    public String interest() {
        return GetValueRequest.class.getName();
    }
}
