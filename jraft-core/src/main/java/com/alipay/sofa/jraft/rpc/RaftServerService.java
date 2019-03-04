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

import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.google.protobuf.Message;

/**
 * Raft RPC service in server.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 4:25:09 PM
 */
public interface RaftServerService {

    /**
     * Handle pre-vote request.
     *
     * @param request   data of the pre vote
     * @return the response message
     */
    Message handlePreVoteRequest(RequestVoteRequest request);

    /**
     * Handle request-vote request.
     *
     * @param request   data of the vote
     * @return the response message
     */
    Message handleRequestVoteRequest(RequestVoteRequest request);

    /**
     * Handle append-entries request, return response message or
     * called done.run() with response.
     *
     * @param request   data of the entries to append
     * @param done      callback
     * @return the response message
     */
    Message handleAppendEntriesRequest(AppendEntriesRequest request, RpcRequestClosure done);

    /**
     * Handle install-snapshot request, return response message or
     * called done.run() with response.
     *
     * @param request   data of the install snapshot request
     * @param done      callback
     * @return the response message
     */
    Message handleInstallSnapshot(InstallSnapshotRequest request, RpcRequestClosure done);

    /**
     * Handle time-out-now request, return response message or
     * called done.run() with response.
     *
     * @param request   data of the timeout now request
     * @param done      callback
     * @return the response message
     */
    Message handleTimeoutNowRequest(TimeoutNowRequest request, RpcRequestClosure done);

    /**
     * Handle read-index request, call the RPC closure with response.
     *
     * @param request   data of the readIndex read
     * @param done      callback
     */
    void handleReadIndexRequest(ReadIndexRequest request, RpcResponseClosure<ReadIndexResponse> done);
}
