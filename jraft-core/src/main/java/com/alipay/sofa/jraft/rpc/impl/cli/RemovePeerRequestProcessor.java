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

import java.util.List;
import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests.RemovePeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.RemovePeerResponse;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.google.protobuf.Message;

/**
 * Remove peer request processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 2:23:40 PM
 */
public class RemovePeerRequestProcessor extends BaseCliRequestProcessor<RemovePeerRequest> {

    public RemovePeerRequestProcessor(Executor executor) {
        super(executor);
    }

    @Override
    protected String getPeerId(RemovePeerRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(RemovePeerRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(CliRequestContext ctx, RemovePeerRequest request, RpcRequestClosure done) {
        List<PeerId> oldPeers = ctx.node.listPeers();
        String removingPeerIdStr = request.getPeerId();
        PeerId removingPeer = new PeerId();
        if (removingPeer.parse(removingPeerIdStr)) {
            LOG.info("Receive RemovePeerRequest to {} from {}, removing {}", ctx.node.getNodeId(), done.getBizContext()
                .getRemoteAddress(), removingPeerIdStr);
            ctx.node.removePeer(removingPeer, status -> {
                if (!status.isOk()) {
                    done.run(status);
                } else {
                    RemovePeerResponse.Builder rb = RemovePeerResponse.newBuilder();
                    for (PeerId oldPeer : oldPeers) {
                        rb.addOldPeers(oldPeer.toString());
                        if (!oldPeer.equals(removingPeer)) {
                            rb.addNewPeers(oldPeer.toString());
                        }
                    }
                    done.sendResponse(rb.build());
                }
            });
        } else {
            return RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to parse peer id %s", removingPeerIdStr);
        }

        return null;
    }

    @Override
    public String interest() {
        return RemovePeerRequest.class.getName();
    }
}
