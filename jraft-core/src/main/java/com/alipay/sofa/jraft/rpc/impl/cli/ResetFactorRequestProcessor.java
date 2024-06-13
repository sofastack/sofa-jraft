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
package com.alipay.sofa.jraft.rpc.impl.cli;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.rpc.CliRequests.ResetFactorResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.ResetFactorRequest;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.google.protobuf.Message;

import java.util.concurrent.Executor;

/**
 * Reset factor request processor
 *
 * @author Akai
 */
public class ResetFactorRequestProcessor extends BaseCliRequestProcessor<ResetFactorRequest> {

    public ResetFactorRequestProcessor(Executor executor) {
        super(executor, ResetFactorResponse.getDefaultInstance());
    }

    @Override
    public String interest() {
        return ResetFactorRequest.class.getName();
    }

    @Override
    protected String getPeerId(ResetFactorRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(ResetFactorRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(CliRequestContext ctx, ResetFactorRequest request, RpcRequestClosure done) {
        Node node = ctx.node;
        int readFactor = request.getReadFactor();
        int writeFactor = request.getWriteFactor();
        LOG.info("Receive AddPeerRequest to {} from {}, change readFactor to {} , writeFactor to {}",
                ctx.node.getNodeId(), done.getRpcCtx().getRemoteAddress(), readFactor, writeFactor);
        node.resetFactor(readFactor, writeFactor, status -> {
            if (!status.isOk()) {
                done.run(status);
            } else {
                final ResetFactorResponse.Builder rb = ResetFactorResponse.newBuilder();
                rb.setReadFactor(readFactor);
                rb.setWriteFactor(writeFactor);
                done.sendResponse(rb.build());
            }
        });
        return null;
    }
}
