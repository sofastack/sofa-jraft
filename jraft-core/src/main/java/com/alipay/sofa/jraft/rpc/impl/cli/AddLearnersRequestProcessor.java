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

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests;
import com.alipay.sofa.jraft.rpc.CliRequests.AddLearnersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.LearnersOpResponse;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;

/**
 * AddLearners request processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * @author jiachun.fjc
 */
public class AddLearnersRequestProcessor extends BaseCliRequestProcessor<AddLearnersRequest> {

    public AddLearnersRequestProcessor(final Executor executor) {
        super(executor, CliRequests.LearnersOpResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final AddLearnersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final AddLearnersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final AddLearnersRequest request,
                                      final RpcRequestClosure done) {
        final List<PeerId> oldLearners = ctx.node.listLearners();
        final List<PeerId> addingLearners = new ArrayList<>(request.getLearnersCount());

        for (final String peerStr : request.getLearnersList()) {
            final PeerId peer = new PeerId();
            if (!peer.parse(peerStr)) {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerStr);
            }
            addingLearners.add(peer);
        }

        LOG.info("Receive AddLearnersRequest to {} from {}, adding {}.", ctx.node.getNodeId(),
            done.getRpcCtx().getRemoteAddress(), addingLearners);
        ctx.node.addLearners(addingLearners, status -> {
            if (!status.isOk()) {
                done.run(status);
            } else {
                final LearnersOpResponse.Builder rb = LearnersOpResponse.newBuilder();

                for (final PeerId peer : oldLearners) {
                    rb.addOldLearners(peer.toString());
                    rb.addNewLearners(peer.toString());
                }

                for (final PeerId peer : addingLearners) {
                    if (!oldLearners.contains(peer)) {
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
        return AddLearnersRequest.class.getName();
    }

}
