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
package com.alipay.sofa.jraft.rpc.impl.core;

import java.util.concurrent.Future;

import com.alipay.remoting.ConnectionEventType;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.impl.AbstractBoltClientService;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;

/**
 * Raft rpc service based bolt.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-28 6:07:05 PM
 */
public class BoltRaftClientService extends AbstractBoltClientService implements RaftClientService {

    // cached node options
    private NodeOptions           nodeOptions;
    private final ReplicatorGroup rgGroup;

    @Override
    protected void configRpcClient(RpcClient rpcClient) {
        rpcClient.addConnectionEventProcessor(ConnectionEventType.CONNECT, new ClientServiceConnectionEventProcessor(
            rgGroup));
    }

    public BoltRaftClientService(ReplicatorGroup rgGroup) {
        this.rgGroup = rgGroup;
    }

    @Override
    public synchronized boolean init(RpcOptions rpcOptions) {
        final boolean ret = super.init(rpcOptions);
        if (ret) {
            nodeOptions = (NodeOptions) rpcOptions;
        }
        return ret;
    }

    @Override
    public Future<Message> preVote(Endpoint endpoint, RequestVoteRequest request,
                                   RpcResponseClosure<RequestVoteResponse> done) {
        return invokeWithDone(endpoint, request, done, nodeOptions.getElectionTimeoutMs());
    }

    @Override
    public Future<Message> requestVote(Endpoint endpoint, RequestVoteRequest request,
                                       RpcResponseClosure<RequestVoteResponse> done) {
        return invokeWithDone(endpoint, request, done, nodeOptions.getElectionTimeoutMs());
    }

    @Override
    public Future<Message> appendEntries(Endpoint endpoint, AppendEntriesRequest request, int timeoutMs,
                                         RpcResponseClosure<AppendEntriesResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    @Override
    public Future<Message> getFile(Endpoint endpoint, GetFileRequest request, int timeoutMs,
                                   RpcResponseClosure<GetFileResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    @Override
    public Future<Message> installSnapshot(Endpoint endpoint, InstallSnapshotRequest request,
                                           RpcResponseClosure<InstallSnapshotResponse> done) {
        return invokeWithDone(endpoint, request, done, rpcOptions.getRpcInstallSnapshotTimeout());
    }

    @Override
    public Future<Message> timeoutNow(Endpoint endpoint, TimeoutNowRequest request, int timeoutMs,
                                      RpcResponseClosure<TimeoutNowResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    @Override
    public Future<Message> readIndex(Endpoint endpoint, ReadIndexRequest request, int timeoutMs,
                                     RpcResponseClosure<ReadIndexResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }
}
