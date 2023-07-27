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
package com.alipay.sofa.jraft.rpc.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import com.alipay.sofa.jraft.rpc.CliRequests;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.google.protobuf.Message;

/**
 * Not thread safe.
 *
 * @author jiachun.fjc
 */
public class MarshallerHelper {

    private static Map<String, Message> messages = new ConcurrentHashMap<>();

    static {
        messages.put(RpcRequests.AppendEntriesRequest.class.getName(),
            RpcRequests.AppendEntriesResponse.getDefaultInstance());
        messages.put(RpcRequests.GetFileRequest.class.getName(), RpcRequests.GetFileResponse.getDefaultInstance());
        messages.put(RpcRequests.InstallSnapshotRequest.class.getName(),
            RpcRequests.InstallSnapshotResponse.getDefaultInstance());
        messages.put(RpcRequests.RequestVoteRequest.class.getName(),
            RpcRequests.RequestVoteResponse.getDefaultInstance());
        messages.put(RpcRequests.PingRequest.class.getName(), RpcRequests.ErrorResponse.getDefaultInstance());
        messages
            .put(RpcRequests.TimeoutNowRequest.class.getName(), RpcRequests.TimeoutNowResponse.getDefaultInstance());
        messages.put(RpcRequests.ReadIndexRequest.class.getName(), RpcRequests.ReadIndexResponse.getDefaultInstance());
        messages.put(CliRequests.AddPeerRequest.class.getName(), CliRequests.AddPeerResponse.getDefaultInstance());
        messages
            .put(CliRequests.RemovePeerRequest.class.getName(), CliRequests.RemovePeerResponse.getDefaultInstance());
        messages.put(CliRequests.ResetPeerRequest.class.getName(), RpcRequests.ErrorResponse.getDefaultInstance());
        messages.put(CliRequests.ChangePeersRequest.class.getName(),
            CliRequests.ChangePeersResponse.getDefaultInstance());
        messages.put(CliRequests.GetLeaderRequest.class.getName(), CliRequests.GetLeaderResponse.getDefaultInstance());
        messages.put(CliRequests.SnapshotRequest.class.getName(), RpcRequests.ErrorResponse.getDefaultInstance());
        messages.put(CliRequests.TransferLeaderRequest.class.getName(), RpcRequests.ErrorResponse.getDefaultInstance());
        messages.put(CliRequests.GetPeersRequest.class.getName(), CliRequests.GetPeersResponse.getDefaultInstance());
        messages.put(CliRequests.AddLearnersRequest.class.getName(),
            CliRequests.LearnersOpResponse.getDefaultInstance());
        messages.put(CliRequests.RemoveLearnersRequest.class.getName(),
            CliRequests.LearnersOpResponse.getDefaultInstance());
        messages.put(CliRequests.ResetLearnersRequest.class.getName(),
            CliRequests.LearnersOpResponse.getDefaultInstance());
    }

    public static Message findRespInstance(final String name) {
        return messages.get(name);
    }

    public static void registerRespInstance(final String name, final Message instance) {
        messages.put(name, instance);
    }
}