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
import com.alipay.sofa.jraft.rpc.CliRequests.AddPeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.AddPeerResponse;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;

/**
 * AddPeer request processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * @author jiachun.fjc
 */
public class AddPeerRequestProcessor extends BaseCliRequestProcessor<AddPeerRequest> {

    public AddPeerRequestProcessor(Executor executor) {
        super(executor, AddPeerResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final AddPeerRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final AddPeerRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final AddPeerRequest request, final RpcRequestClosure done) {
        final List<PeerId> oldPeers = ctx.node.listPeers();
        final String addingPeerIdStr = request.getPeerId();
        final PeerId addingPeer = new PeerId();
        if (addingPeer.parse(addingPeerIdStr)) {
            LOG.info("Receive AddPeerRequest to {} from {}, adding {}", ctx.node.getNodeId(), done.getRpcCtx()
                .getRemoteAddress(), addingPeerIdStr);
            ctx.node.addPeer(addingPeer, status -> {
                if (!status.isOk()) {
                    done.run(status);
                } else {
                    final AddPeerResponse.Builder rb = AddPeerResponse.newBuilder();
                    boolean alreadyExists = false;
                    for (final PeerId oldPeer : oldPeers) {
                        rb.addOldPeers(oldPeer.toString());
                        rb.addNewPeers(oldPeer.toString());
                        if (oldPeer.equals(addingPeer)) {
                            alreadyExists = true;
                        }
                    }
                    if (!alreadyExists) {
                        rb.addNewPeers(addingPeerIdStr);
                    }
                    done.sendResponse(rb.build());
                }
            });
        } else {
            return RpcFactoryHelper //
                .responseFactory() //
                .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", addingPeerIdStr);
        }

        return null;
    }

    @Override
    public String interest() {
        return AddPeerRequest.class.getName();
    }

}
