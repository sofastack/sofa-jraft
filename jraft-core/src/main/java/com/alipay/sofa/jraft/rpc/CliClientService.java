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
package com.alipay.sofa.jraft.rpc;

import java.util.concurrent.Future;

import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;

/**
 * Cli RPC client service.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 11:15:13 AM
 */
public interface CliClientService extends ClientService {

    /**
     * Adds a peer.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> addPeer(Endpoint endpoint, CliRequests.AddPeerRequest request,
                            RpcResponseClosure<CliRequests.AddPeerResponse> done);

    /**
     * Removes a peer.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> removePeer(Endpoint endpoint, CliRequests.RemovePeerRequest request,
                               RpcResponseClosure<CliRequests.RemovePeerResponse> done);

    /**
     * Reset a peer.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> resetPeer(Endpoint endpoint, CliRequests.ResetPeerRequest request,
                              RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Do a snapshot.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> snapshot(Endpoint endpoint, CliRequests.SnapshotRequest request,
                             RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Change peers.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> changePeers(Endpoint endpoint, CliRequests.ChangePeersRequest request,
                                RpcResponseClosure<CliRequests.ChangePeersResponse> done);

    /**
     * Add learners
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     * @since 1.3.0
     */
    Future<Message> addLearners(Endpoint endpoint, CliRequests.AddLearnersRequest request,
                                RpcResponseClosure<CliRequests.LearnersOpResponse> done);

    /**
     * Remove learners
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     * @since 1.3.0
     */
    Future<Message> removeLearners(Endpoint endpoint, CliRequests.RemoveLearnersRequest request,
                                   RpcResponseClosure<CliRequests.LearnersOpResponse> done);

    /**
     * Reset learners
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     * @since 1.3.0
     */
    Future<Message> resetLearners(Endpoint endpoint, CliRequests.ResetLearnersRequest request,
                                  RpcResponseClosure<CliRequests.LearnersOpResponse> done);

    /**
     * Get the group leader.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> getLeader(Endpoint endpoint, CliRequests.GetLeaderRequest request,
                              RpcResponseClosure<CliRequests.GetLeaderResponse> done);

    /**
     * Transfer leadership to other peer.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> transferLeader(Endpoint endpoint, CliRequests.TransferLeaderRequest request,
                                   RpcResponseClosure<RpcRequests.ErrorResponse> done);

    /**
     * Get all peers of the replication group.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Message> getPeers(Endpoint endpoint, CliRequests.GetPeersRequest request,
                             RpcResponseClosure<CliRequests.GetPeersResponse> done);
}
