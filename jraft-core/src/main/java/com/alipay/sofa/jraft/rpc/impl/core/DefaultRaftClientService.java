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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcClient;
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
import com.alipay.sofa.jraft.rpc.impl.AbstractClientService;
import com.alipay.sofa.jraft.rpc.impl.FutureImpl;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.DefaultFixedThreadsExecutorGroupFactory;
import com.alipay.sofa.jraft.util.concurrent.FixedThreadsExecutorGroup;
import com.google.protobuf.Message;

/**
 * Raft rpc service based bolt.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * @author jiachun.fjc
 */
public class DefaultRaftClientService extends AbstractClientService implements RaftClientService {

    private final FixedThreadsExecutorGroup         appendEntriesExecutors;

    private final ConcurrentMap<Endpoint, Executor> appendEntriesExecutorMap = new ConcurrentHashMap<>();

    // cached node options
    private NodeOptions                             nodeOptions;
    private final ReplicatorGroup                   rgGroup;

    @Override
    protected void configRpcClient(final RpcClient rpcClient) {
        rpcClient.registerConnectEventListener(this.rgGroup);
    }

    public DefaultRaftClientService(final ReplicatorGroup rgGroup) {
        this(rgGroup, DefaultFixedThreadsExecutorGroupFactory.INSTANCE.newExecutorGroup(
            Utils.APPEND_ENTRIES_THREADS_SEND, "Append-Entries-Thread-Send", Utils.MAX_APPEND_ENTRIES_TASKS_PER_THREAD,
            true));
    }

    public DefaultRaftClientService(final ReplicatorGroup rgGroup,
                                    final FixedThreadsExecutorGroup customAppendEntriesExecutors) {
        this.rgGroup = rgGroup;
        this.appendEntriesExecutors = customAppendEntriesExecutors;
    }

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        final boolean ret = super.init(rpcOptions);
        if (ret) {
            this.nodeOptions = (NodeOptions) rpcOptions;
        }
        return ret;
    }

    @Override
    public Future<Message> preVote(final Endpoint endpoint, final RequestVoteRequest request,
                                   final RpcResponseClosure<RequestVoteResponse> done) {
        if (!checkConnection(endpoint, true)) {
            return onConnectionFail(endpoint, request, done, this.rpcExecutor);
        }

        return invokeWithDone(endpoint, request, done, this.nodeOptions.getElectionTimeoutMs());
    }

    @Override
    public Future<Message> requestVote(final Endpoint endpoint, final RequestVoteRequest request,
                                       final RpcResponseClosure<RequestVoteResponse> done) {
        if (!checkConnection(endpoint, true)) {
            return onConnectionFail(endpoint, request, done, this.rpcExecutor);
        }

        return invokeWithDone(endpoint, request, done, this.nodeOptions.getElectionTimeoutMs());
    }

    @Override
    public Future<Message> appendEntries(final Endpoint endpoint, final AppendEntriesRequest request,
                                         final int timeoutMs, final RpcResponseClosure<AppendEntriesResponse> done) {
        final Executor executor = this.appendEntriesExecutorMap.computeIfAbsent(endpoint, k -> appendEntriesExecutors.next());

        if (!checkConnection(endpoint, true)) {
            return onConnectionFail(endpoint, request, done, executor);
        }

        return invokeWithDone(endpoint, request, done, timeoutMs, executor);
    }

    @Override
    public Future<Message> getFile(final Endpoint endpoint, final GetFileRequest request, final int timeoutMs,
                                   final RpcResponseClosure<GetFileResponse> done) {
        // open checksum
        final InvokeContext ctx = new InvokeContext();
        ctx.put(InvokeContext.CRC_SWITCH, true);
        return invokeWithDone(endpoint, request, ctx, done, timeoutMs);
    }

    @Override
    public Future<Message> installSnapshot(final Endpoint endpoint, final InstallSnapshotRequest request,
                                           final RpcResponseClosure<InstallSnapshotResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcInstallSnapshotTimeout());
    }

    @Override
    public Future<Message> timeoutNow(final Endpoint endpoint, final TimeoutNowRequest request, final int timeoutMs,
                                      final RpcResponseClosure<TimeoutNowResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    @Override
    public Future<Message> readIndex(final Endpoint endpoint, final ReadIndexRequest request, final int timeoutMs,
                                     final RpcResponseClosure<ReadIndexResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    // fail-fast when no connection
    private Future<Message> onConnectionFail(final Endpoint endpoint, final Message request, Closure done, final Executor executor) {
        final FutureImpl<Message> future = new FutureImpl<>();
        executor.execute(() -> {
            final String fmt = "Check connection[%s] fail and try to create new one";
            if (done != null) {
                try {
                    done.run(new Status(RaftError.EINTERNAL, fmt, endpoint));
                } catch (final Throwable t) {
                    LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                }
            }
            if (!future.isDone()) {
                future.failure(new RemotingException(String.format(fmt, endpoint)));
            }
        });
        return future;
    }
}
