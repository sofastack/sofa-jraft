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
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests.ResetPeerRequest;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;

/**
 * Reset peer request processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * @author jiachun.fjc
 */
public class ResetPeerRequestProcessor extends BaseCliRequestProcessor<ResetPeerRequest> {

    public ResetPeerRequestProcessor(Executor executor) {
        super(executor, RpcRequests.ErrorResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final ResetPeerRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final ResetPeerRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final ResetPeerRequest request,
                                      final RpcRequestClosure done) {
        final Configuration newConf = new Configuration();
        for (final String peerIdStr : request.getNewPeersList()) {
            final PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                newConf.addPeer(peer);
            } else {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        }
        LOG.info("Receive ResetPeerRequest to {} from {}, new conf is {}", ctx.node.getNodeId(), done.getRpcCtx()
            .getRemoteAddress(), newConf);
        final Status st = ctx.node.resetPeers(newConf);
        return RpcFactoryHelper //
            .responseFactory() //
            .newResponse(defaultResp(), st);
    }

    @Override
    public String interest() {
        return ResetPeerRequest.class.getName();
    }

}
