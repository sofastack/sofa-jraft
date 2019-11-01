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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.Url;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.exception.InvokeTimeoutException;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.ClientService;
import com.alipay.sofa.jraft.rpc.ProtobufMsgFactory;
import com.alipay.sofa.jraft.rpc.RpcRequests.ErrorResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.PingRequest;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.impl.core.JRaftRpcAddressParser;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolMetricSet;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;

/**
 * Abstract RPC client service based on bolt.

 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 3:27:33 PM
 */
public abstract class AbstractBoltClientService implements ClientService {

    protected static final Logger   LOG = LoggerFactory.getLogger(AbstractBoltClientService.class);

    static {
        ProtobufMsgFactory.load();
    }

    protected volatile RpcClient    rpcClient;
    protected ThreadPoolExecutor    rpcExecutor;
    protected RpcOptions            rpcOptions;
    protected JRaftRpcAddressParser rpcAddressParser;
    protected InvokeContext         defaultInvokeCtx;

    public RpcClient getRpcClient() {
        return this.rpcClient;
    }

    @Override
    public boolean isConnected(final Endpoint endpoint) {
        final RpcClient rc = this.rpcClient;
        return rc != null && isConnected(rc, endpoint);
    }

    private static boolean isConnected(final RpcClient rpcClient, final Endpoint endpoint) {
        return rpcClient.checkConnection(endpoint.toString());
    }

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        if (this.rpcClient != null) {
            return true;
        }
        this.rpcOptions = rpcOptions;
        this.rpcAddressParser = new JRaftRpcAddressParser();
        this.defaultInvokeCtx = new InvokeContext();
        this.defaultInvokeCtx.put(InvokeContext.BOLT_CRC_SWITCH, this.rpcOptions.isEnableRpcChecksum());
        return initRpcClient(this.rpcOptions.getRpcProcessorThreadPoolSize());
    }

    protected void configRpcClient(final RpcClient rpcClient) {
        // NO-OP
    }

    protected boolean initRpcClient(final int rpcProcessorThreadPoolSize) {
        this.rpcClient = new RpcClient();
        configRpcClient(this.rpcClient);
        this.rpcClient.init();
        this.rpcExecutor = ThreadPoolUtil.newBuilder() //
            .poolName("JRaft-RPC-Processor") //
            .enableMetric(true) //
            .coreThreads(rpcProcessorThreadPoolSize / 3) //
            .maximumThreads(rpcProcessorThreadPoolSize) //
            .keepAliveSeconds(60L) //
            .workQueue(new ArrayBlockingQueue<>(10000)) //
            .threadFactory(new NamedThreadFactory("JRaft-RPC-Processor-", true)) //
            .build();
        if (this.rpcOptions.getMetricRegistry() != null) {
            this.rpcOptions.getMetricRegistry().register("raft-rpc-client-thread-pool",
                new ThreadPoolMetricSet(this.rpcExecutor));
            Utils.registerClosureExecutorMetrics(this.rpcOptions.getMetricRegistry());
        }
        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.rpcClient != null) {
            this.rpcClient.shutdown();
            this.rpcClient = null;
            this.rpcExecutor.shutdown();
        }
    }

    @Override
    public boolean connect(final Endpoint endpoint) {
        final RpcClient rc = this.rpcClient;
        if (rc == null) {
            throw new IllegalStateException("Client service is uninitialized.");
        }
        if (isConnected(rc, endpoint)) {
            return true;
        }
        try {
            final PingRequest req = PingRequest.newBuilder() //
                .setSendTimestamp(System.currentTimeMillis()) //
                .build();
            final ErrorResponse resp = (ErrorResponse) rc.invokeSync(endpoint.toString(), req, this.defaultInvokeCtx,
                this.rpcOptions.getRpcConnectTimeoutMs());
            return resp.getErrorCode() == 0;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (final RemotingException e) {
            LOG.error("Fail to connect {}, remoting exception: {}.", endpoint, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean disconnect(final Endpoint endpoint) {
        final RpcClient rc = this.rpcClient;
        if (rc == null) {
            return true;
        }
        LOG.info("Disconnect from {}.", endpoint);
        rc.closeConnection(endpoint.toString());
        return true;
    }

    @Override
    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                              final RpcResponseClosure<T> done, final int timeoutMs) {
        return invokeWithDone(endpoint, request, this.defaultInvokeCtx, done, timeoutMs, this.rpcExecutor);
    }

    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                              final RpcResponseClosure<T> done, final int timeoutMs,
                                                              final Executor rpcExecutor) {
        return invokeWithDone(endpoint, request, this.defaultInvokeCtx, done, timeoutMs, rpcExecutor);
    }

    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                              final InvokeContext ctx,
                                                              final RpcResponseClosure<T> done, final int timeoutMs) {
        return invokeWithDone(endpoint, request, ctx, done, timeoutMs, this.rpcExecutor);
    }

    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                              final InvokeContext ctx,
                                                              final RpcResponseClosure<T> done, final int timeoutMs,
                                                              final Executor rpcExecutor) {
        final RpcClient rc = this.rpcClient;

        final FutureImpl<Message> future = new FutureImpl<>();
        try {
            if (rc == null) {
                future.failure(new IllegalStateException("Client service is uninitialized."));
                // should be in another thread to avoid dead locking.
                Utils.runClosureInThread(done, new Status(RaftError.EINTERNAL, "Client service is uninitialized."));
                return future;
            }
            final Url rpcUrl = this.rpcAddressParser.parse(endpoint.toString());
            rc.invokeWithCallback(rpcUrl, request, ctx, new InvokeCallback() {

                @SuppressWarnings("unchecked")
                @Override
                public void onResponse(final Object result) {
                    if (future.isCancelled()) {
                        onCanceled(request, done);
                        return;
                    }
                    Status status = Status.OK();
                    if (result instanceof ErrorResponse) {
                        final ErrorResponse eResp = (ErrorResponse) result;
                        status = new Status();
                        status.setCode(eResp.getErrorCode());
                        if (eResp.hasErrorMsg()) {
                            status.setErrorMsg(eResp.getErrorMsg());
                        }
                    } else {
                        if (done != null) {
                            done.setResponse((T) result);
                        }
                    }
                    if (done != null) {
                        try {
                            done.run(status);
                        } catch (final Throwable t) {
                            LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                        }
                    }
                    if (!future.isDone()) {
                        future.setResult((Message) result);
                    }
                }

                @Override
                public void onException(final Throwable e) {
                    if (future.isCancelled()) {
                        onCanceled(request, done);
                        return;
                    }
                    if (done != null) {
                        try {
                            done.run(new Status(e instanceof InvokeTimeoutException ? RaftError.ETIMEDOUT
                                : RaftError.EINTERNAL, "RPC exception:" + e.getMessage()));
                        } catch (final Throwable t) {
                            LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                        }
                    }
                    if (!future.isDone()) {
                        future.failure(e);
                    }
                }

                @Override
                public Executor getExecutor() {
                    return rpcExecutor != null ? rpcExecutor : AbstractBoltClientService.this.rpcExecutor;
                }
            }, timeoutMs <= 0 ? this.rpcOptions.getRpcDefaultTimeout() : timeoutMs);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            future.failure(e);
            // should be in another thread to avoid dead locking.
            Utils.runClosureInThread(done, new Status(RaftError.EINTR, "Sending rpc was interrupted"));
        } catch (final RemotingException e) {
            future.failure(e);
            // should be in another thread to avoid dead locking.
            Utils.runClosureInThread(done,
                new Status(RaftError.EINTERNAL, "Fail to send a RPC request:" + e.getMessage()));

        }
        return future;
    }

    private <T extends Message> void onCanceled(final Message request, final RpcResponseClosure<T> done) {
        if (done != null) {
            try {
                done.run(new Status(RaftError.ECANCELED, "RPC request was canceled by future."));
            } catch (final Throwable t) {
                LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
            }
        }
    }

}
