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
package com.alipay.sofa.jraft.storage.snapshot.remote;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.Scheduler;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CopyOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.rpc.RpcUtils;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.Message;

/**
 * Copy session.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 12:01:23 PM
 */
@ThreadSafe
public class CopySession implements Session {

    private static final Logger          LOG         = LoggerFactory.getLogger(CopySession.class);

    private final Lock                   lock        = new ReentrantLock();
    private final Status                 st          = Status.OK();
    private final CountDownLatch         finishLatch = new CountDownLatch(1);
    private final GetFileResponseClosure done        = new GetFileResponseClosure();
    private final RaftClientService      rpcService;
    private final GetFileRequest.Builder requestBuilder;
    private final Endpoint               endpoint;
    private final Scheduler              timerManager;
    private final SnapshotThrottle       snapshotThrottle;
    private final RaftOptions            raftOptions;
    private int                          retryTimes  = 0;
    private boolean                      finished;
    private ByteBufferCollector          destBuf;
    private CopyOptions                  copyOptions = new CopyOptions();
    private OutputStream                 outputStream;
    private ScheduledFuture<?>           timer;
    private String                       destPath;
    private Future<Message>              rpcCall;

    /**
     * Get file response closure to answer client.
     *
     * @author boyan (boyan@alibaba-inc.com)
     */
    private class GetFileResponseClosure extends RpcResponseClosureAdapter<GetFileResponse> {

        @Override
        public void run(final Status status) {
            onRpcReturned(status, getResponse());
        }
    }

    public void setDestPath(final String destPath) {
        this.destPath = destPath;
    }

    @OnlyForTest
    GetFileResponseClosure getDone() {
        return this.done;
    }

    @OnlyForTest
    Future<Message> getRpcCall() {
        return this.rpcCall;
    }

    @OnlyForTest
    ScheduledFuture<?> getTimer() {
        return this.timer;
    }

    @Override
    public void close() throws IOException {
        this.lock.lock();
        try {
            if (!this.finished) {
                Utils.closeQuietly(this.outputStream);
            }
            if (null != this.destBuf) {
                this.destBuf.recycle();
                this.destBuf = null;
            }
        } finally {
            this.lock.unlock();
        }
    }

    public CopySession(final RaftClientService rpcService, final Scheduler timerManager,
                       final SnapshotThrottle snapshotThrottle, final RaftOptions raftOptions,
                       final GetFileRequest.Builder rb, final Endpoint ep) {
        super();
        this.snapshotThrottle = snapshotThrottle;
        this.raftOptions = raftOptions;
        this.timerManager = timerManager;
        this.rpcService = rpcService;
        this.requestBuilder = rb;
        this.endpoint = ep;
    }

    public void setDestBuf(final ByteBufferCollector bufRef) {
        this.destBuf = bufRef;
    }

    public void setCopyOptions(final CopyOptions copyOptions) {
        this.copyOptions = copyOptions;
    }

    public void setOutputStream(final OutputStream out) {
        this.outputStream = out;
    }

    @Override
    public void cancel() {
        this.lock.lock();
        try {
            if (this.finished) {
                return;
            }
            if (this.timer != null) {
                this.timer.cancel(true);
            }
            if (this.rpcCall != null) {
                this.rpcCall.cancel(true);
            }
            if (this.st.isOk()) {
                this.st.setError(RaftError.ECANCELED, RaftError.ECANCELED.name());
            }
            onFinished();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void join() throws InterruptedException {
        this.finishLatch.await();
    }

    @Override
    public Status status() {
        return this.st;
    }

    private void onFinished() {
        if (!this.finished) {
            if (!this.st.isOk()) {
                LOG.error("Fail to copy data, readerId={} fileName={} offset={} status={}",
                    this.requestBuilder.getReaderId(), this.requestBuilder.getFilename(),
                    this.requestBuilder.getOffset(), this.st);
            }
            if (this.outputStream != null) {
                Utils.closeQuietly(this.outputStream);
                this.outputStream = null;
            }
            if (this.destBuf != null) {
                final ByteBuffer buf = this.destBuf.getBuffer();
                if (buf != null) {
                    buf.flip();
                }
                this.destBuf = null;
            }
            this.finished = true;
            this.finishLatch.countDown();
        }
    }

    private void onTimer() {
        RpcUtils.runInThread(this::sendNextRpc);
    }

    void onRpcReturned(final Status status, final GetFileResponse response) {
        this.lock.lock();
        try {
            if (this.finished) {
                return;
            }
            if (!status.isOk()) {
                // Reset count to make next rpc retry the previous one
                this.requestBuilder.setCount(0);
                if (status.getCode() == RaftError.ECANCELED.getNumber()) {
                    if (this.st.isOk()) {
                        this.st.setError(status.getCode(), status.getErrorMsg());
                        onFinished();
                        return;
                    }
                }

                // Throttled reading failure does not increase _retry_times
                if (status.getCode() != RaftError.EAGAIN.getNumber()
                        && ++this.retryTimes >= this.copyOptions.getMaxRetry()) {
                    if (this.st.isOk()) {
                        this.st.setError(status.getCode(), status.getErrorMsg());
                        onFinished();
                        return;
                    }
                }
                this.timer = this.timerManager.schedule(this::onTimer, this.copyOptions.getRetryIntervalMs(),
                    TimeUnit.MILLISECONDS);
                return;
            }
            this.retryTimes = 0;
            Requires.requireNonNull(response, "response");
            // Reset count to |real_read_size| to make next rpc get the right offset
            if (!response.getEof()) {
                this.requestBuilder.setCount(response.getReadSize());
            }
            if (this.outputStream != null) {
                try {
                    response.getData().writeTo(this.outputStream);
                } catch (final IOException e) {
                    LOG.error("Fail to write into file {}", this.destPath, e);
                    this.st.setError(RaftError.EIO, RaftError.EIO.name());
                    onFinished();
                    return;
                }
            } else {
                this.destBuf.put(response.getData().asReadOnlyByteBuffer());
            }
            if (response.getEof()) {
                onFinished();
                return;
            }
        } finally {
            this.lock.unlock();
        }
        sendNextRpc();
    }

    /**
     * Send next RPC request to get a piece of file data.
     */
    void sendNextRpc() {
        this.lock.lock();
        try {
            this.timer = null;
            final long offset = this.requestBuilder.getOffset() + this.requestBuilder.getCount();
            final long maxCount = this.destBuf == null ? this.raftOptions.getMaxByteCountPerRpc() : Integer.MAX_VALUE;
            this.requestBuilder.setOffset(offset).setCount(maxCount).setReadPartly(true);

            if (this.finished) {
                return;
            }
            // throttle
            long newMaxCount = maxCount;
            if (this.snapshotThrottle != null) {
                newMaxCount = this.snapshotThrottle.throttledByThroughput(maxCount);
                if (newMaxCount == 0) {
                    // Reset count to make next rpc retry the previous one
                    this.requestBuilder.setCount(0);
                    this.timer = this.timerManager.schedule(this::onTimer, this.copyOptions.getRetryIntervalMs(),
                        TimeUnit.MILLISECONDS);
                    return;
                }
            }
            this.requestBuilder.setCount(newMaxCount);
            final RpcRequests.GetFileRequest request = this.requestBuilder.build();
            LOG.debug("Send get file request {} to peer {}", request, this.endpoint);
            this.rpcCall = this.rpcService.getFile(this.endpoint, request, this.copyOptions.getTimeoutMs(), this.done);
        } finally {
            this.lock.unlock();
        }
    }
}
