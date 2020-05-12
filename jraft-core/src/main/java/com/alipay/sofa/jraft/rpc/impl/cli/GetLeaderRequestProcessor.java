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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests.GetLeaderRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.GetLeaderResponse;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;

/**
 * Process get leader request.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * @author jiachun.fjc
 */
public class GetLeaderRequestProcessor extends BaseCliRequestProcessor<GetLeaderRequest> {

    public GetLeaderRequestProcessor(Executor executor) {
        super(executor, GetLeaderResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final GetLeaderRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final GetLeaderRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final GetLeaderRequest request,
                                      final RpcRequestClosure done) {
        // ignore
        return null;
    }

    @Override
    public Message processRequest(final GetLeaderRequest request, final RpcRequestClosure done) {
        List<Node> nodes = new ArrayList<>();
        final String groupId = getGroupId(request);
        if (request.hasPeerId()) {
            final String peerIdStr = getPeerId(request);
            final PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                final Status st = new Status();
                nodes.add(getNode(groupId, peer, st));
                if (!st.isOk()) {
                    return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(defaultResp(), st);
                }
            } else {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        } else {
            nodes = NodeManager.getInstance().getNodesByGroupId(groupId);
        }
        if (nodes == null || nodes.isEmpty()) {
            return RpcFactoryHelper //
                .responseFactory() //
                .newResponse(defaultResp(), RaftError.ENOENT, "No nodes in group %s", groupId);
        }
        for (final Node node : nodes) {
            final PeerId leader = node.getLeaderId();
            if (leader != null && !leader.isEmpty()) {
                return GetLeaderResponse.newBuilder().setLeaderId(leader.toString()).build();
            }
        }
        return RpcFactoryHelper //
            .responseFactory() //
            .newResponse(defaultResp(), RaftError.EAGAIN, "Unknown leader");
    }

    @Override
    public String interest() {
        return GetLeaderRequest.class.getName();
    }

}
