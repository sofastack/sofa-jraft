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

import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests.TransferLeaderRequest;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.google.protobuf.Message;

/**
 * Snapshot request processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 2:41:27 PM
 */
public class TransferLeaderRequestProcessor extends BaseCliRequestProcessor<TransferLeaderRequest> {

    public TransferLeaderRequestProcessor(Executor executor) {
        super(executor);
    }

    @Override
    protected String getPeerId(TransferLeaderRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(TransferLeaderRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(CliRequestContext ctx, TransferLeaderRequest request, RpcRequestClosure done) {
        PeerId peer = new PeerId();
        if (request.hasPeerId() && !peer.parse(request.getPeerId())) {
            return RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to parse peer id %s", request.getPeerId());
        }
        LOG.info("Receive TransferLeaderRequest to {} from {} , newLeader will be {}", ctx.node.getNodeId(), done
            .getBizContext().getRemoteAddress(), peer);
        Status st = ctx.node.transferLeadershipTo(peer);
        return RpcResponseFactory.newResponse(st);
    }

    @Override
    public String interest() {
        return TransferLeaderRequest.class.getName();
    }

}
