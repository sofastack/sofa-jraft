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

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.example.counter.CounterServer;
import com.alipay.sofa.jraft.example.counter.IncrementAndAddClosure;

/**
 * IncrementAndGetRequest processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 5:43:57 PM
 */
public class IncrementAndGetRequestProcessor extends AsyncUserProcessor<IncrementAndGetRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementAndGetRequestProcessor.class);

    private final CounterServer counterServer;

    public IncrementAndGetRequestProcessor(CounterServer counterServer) {
        super();
        this.counterServer = counterServer;
    }

    @Override
    public void handleRequest(final BizContext bizCtx, final AsyncContext asyncCtx, final IncrementAndGetRequest request) {
        if (!this.counterServer.getFsm().isLeader()) {
            asyncCtx.sendResponse(this.counterServer.redirect());
            return;
        }

        final ValueResponse response = new ValueResponse();
        final IncrementAndAddClosure closure = new IncrementAndAddClosure(counterServer, request, response,
                status -> {
                    if (!status.isOk()) {
                        response.setErrorMsg(status.getErrorMsg());
                        response.setSuccess(false);
                    }
                    asyncCtx.sendResponse(response);
                });

        try {
            final Task task = new Task();
            task.setDone(closure);
            task.setData(ByteBuffer
                .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(request)));

            // apply task to raft group.
            counterServer.getNode().apply(task);
        } catch (final CodecException e) {
            LOG.error("Fail to encode IncrementAndGetRequest", e);
            response.setSuccess(false);
            response.setErrorMsg(e.getMessage());
            asyncCtx.sendResponse(response);
        }
    }

    @Override
    public String interest() {
        return IncrementAndGetRequest.class.getName();
    }
}
