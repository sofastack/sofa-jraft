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
import java.util.Map;
import java.util.concurrent.Executor;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.CliRequests.GetPeersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.GetPeersResponse;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.google.protobuf.Message;

/**
 * Process get all peers of the replication group request.
 *
 * @author jiachun.fjc
 */
public class GetPeersRequestProcessor extends BaseCliRequestProcessor<GetPeersRequest> {

    public GetPeersRequestProcessor(final Executor executor) {
        super(executor, GetPeersResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final GetPeersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final GetPeersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final GetPeersRequest request,
                                      final RpcRequestClosure done) {
        final List<PeerId> peers;
        final List<PeerId> learners;
        Map<PeerId, PeerId> learnerWithSource = null;
        if (request.hasOnlyAlive() && request.getOnlyAlive()) {
            peers = ctx.node.listAlivePeers();
            learners = ctx.node.listAliveLearners();
        } else {
            peers = ctx.node.listPeers();
            learnerWithSource = ctx.node.listLearners();
            learners = new ArrayList<>(learnerWithSource.keySet());
        }
        final GetPeersResponse.Builder builder = GetPeersResponse.newBuilder();
        for (final PeerId peerId : peers) {
            builder.addPeers(peerId.toString());
        }
        for (final PeerId peerId : learners) {
            builder.addLearners(peerId.toString());
        }
        if (learnerWithSource != null && !learnerWithSource.isEmpty()) {
            for (Map.Entry<PeerId, PeerId> entry : learnerWithSource.entrySet()) {
                builder.putLearnerWithSource(entry.getKey().toString(), entry.getValue().toString());
            }
        }
        return builder.build();
    }

    @Override
    public String interest() {
        return GetPeersRequest.class.getName();
    }
}
