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
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests.LearnersOpResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.RemoveLearnersRequest;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;

/**
 * RemoveLearners request processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 */
public class RemoveLearnersRequestProcessor extends BaseCliRequestProcessor<RemoveLearnersRequest> {

    public RemoveLearnersRequestProcessor(final Executor executor) {
        super(executor, LearnersOpResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final RemoveLearnersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final RemoveLearnersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final RemoveLearnersRequest request,
                                      final RpcRequestClosure done) {
        final Map<PeerId, PeerId> oldLearners = ctx.node.listLearners();
        final List<PeerId> removeingLearners = new ArrayList<>(request.getLearnersCount());

        for (final String peerStr : request.getLearnersList()) {
            final PeerId peer = new PeerId();
            if (!peer.parse(peerStr)) {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerStr);
            }
            removeingLearners.add(peer);
        }

        LOG.info("Receive RemoveLearnersRequest to {} from {}, removing {}.", ctx.node.getNodeId(),
            done.getRpcCtx().getRemoteAddress(), removeingLearners);
        ctx.node.removeLearners(removeingLearners, status -> {
            if (!status.isOk()) {
                done.run(status);
            } else {
                final LearnersOpResponse.Builder rb = LearnersOpResponse.newBuilder();

                for (final PeerId peer : oldLearners.keySet()) {
                    rb.addOldLearners(peer.toString());
                    if (!removeingLearners.contains(peer)) {
                        rb.addNewLearners(peer.toString());
                    }
                }

                done.sendResponse(rb.build());
            }
        });

        return null;
    }

    @Override
    public String interest() {
        return RemoveLearnersRequest.class.getName();
    }

}
