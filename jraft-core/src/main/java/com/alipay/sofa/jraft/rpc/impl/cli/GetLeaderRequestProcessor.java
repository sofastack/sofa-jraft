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
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.google.protobuf.Message;

/**
 * Process get leader request.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 2:43:20 PM
 */
public class GetLeaderRequestProcessor extends BaseCliRequestProcessor<GetLeaderRequest> {

    public GetLeaderRequestProcessor(Executor executor) {
        super(executor);
    }

    @Override
    protected String getPeerId(GetLeaderRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(GetLeaderRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(CliRequestContext ctx, GetLeaderRequest request, RpcRequestClosure done) {
        //ignore
        return null;
    }

    @Override
    public Message processRequest(GetLeaderRequest request, RpcRequestClosure done) {
        List<Node> nodes = new ArrayList<>();
        String groupId = getGroupId(request);
        if (request.hasPeerId()) {
            String peerIdStr = getPeerId(request);
            PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                Status st = new Status();
                nodes.add(getNode(groupId, peer, st));
                if (!st.isOk()) {
                    return RpcResponseFactory.newResponse(st);
                }
            } else {
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Fail to parse peer id %", peerIdStr);
            }
        } else {
            nodes = NodeManager.getInstance().getNodesByGroupId(groupId);
        }
        if (nodes == null || nodes.isEmpty()) {
            return RpcResponseFactory.newResponse(RaftError.ENOENT, "No nodes in group %s", groupId);
        }
        for (Node node : nodes) {
            PeerId leader = node.getLeaderId();
            if (leader != null && !leader.isEmpty()) {
                return GetLeaderResponse.newBuilder().setLeaderId(leader.toString()).build();
            }
        }
        return RpcResponseFactory.newResponse(RaftError.EAGAIN, "Unknown leader");
    }

    @Override
    public String interest() {
        return GetLeaderRequest.class.getName();
    }

}
