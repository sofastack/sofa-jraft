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

import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.TimerManager;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CopyOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.impl.FutureImpl;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(value = MockitoJUnitRunner.class)
public class CopySessionTest {
    private CopySession                        session;
    @Mock
    private RaftClientService                  rpcService;
    private RpcRequests.GetFileRequest.Builder rb;
    private final Endpoint                     address = new Endpoint("localhost", 8081);
    private CopyOptions                        copyOpts;
    private RaftOptions                        raftOpts;
    private TimerManager                       timerManager;

    @Before
    public void setup() {
        this.timerManager = new TimerManager(5);
        this.copyOpts = new CopyOptions();
        this.rb = RpcRequests.GetFileRequest.newBuilder();
        this.rb.setReaderId(99);
        this.rb.setFilename("data");
        this.raftOpts = new RaftOptions();
        this.session = new CopySession(rpcService, timerManager, null, raftOpts, rb, address);
        this.session.setCopyOptions(copyOpts);
    }

    @After
    public void teardown() {
        Utils.closeQuietly(this.session);
        this.timerManager.shutdown();
    }

    @Test
    public void testSendNextRpc() {
        final int maxCount = this.raftOpts.getMaxByteCountPerRpc();
        sendNextRpc(maxCount);
    }

    @Test
    public void testSendNextRpcWithBuffer() {
        session.setDestBuf(ByteBufferCollector.allocate(1));
        final int maxCount = Integer.MAX_VALUE;
        sendNextRpc(maxCount);
    }

    @Test
    public void testOnRpcReturnedEOF() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                try {
                    //test join, should return
                    session.join();
                    latch.countDown();
                } catch (final InterruptedException e) {

                }
            }
        }.start();
        assertNull(this.session.getRpcCall());
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(0);
        this.session.setDestBuf(bufRef);

        this.session.onRpcReturned(Status.OK(), RpcRequests.GetFileResponse.newBuilder().setReadSize(100).setEof(true)
            .setData(ByteString.copyFrom(new byte[100])).build());
        assertEquals(100, bufRef.capacity());
        //should be flip
        assertEquals(0, bufRef.getBuffer().position());
        assertEquals(100, bufRef.getBuffer().remaining());

        assertNull(this.session.getRpcCall());
        latch.await();
    }

    @Test
    public void testOnRpcReturnedOK() {
        assertNull(this.session.getRpcCall());
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(0);
        this.session.setDestBuf(bufRef);

        final FutureImpl<Message> future = new FutureImpl<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename("data").setCount(Integer.MAX_VALUE).setOffset(100).setReadPartly(true);
        Mockito
            .when(this.rpcService.getFile(this.address, rb.build(), this.copyOpts.getTimeoutMs(), session.getDone()))
            .thenReturn(future);

        this.session.onRpcReturned(Status.OK(), RpcRequests.GetFileResponse.newBuilder().setReadSize(100).setEof(false)
            .setData(ByteString.copyFrom(new byte[100])).build());
        assertEquals(100, bufRef.capacity());
        assertEquals(100, bufRef.getBuffer().position());

        assertNotNull(this.session.getRpcCall());
        //send next request
        assertSame(future, this.session.getRpcCall());
    }

    @Test
    public void testOnRpcReturnedRetry() throws Exception {
        assertNull(this.session.getTimer());
        assertNull(this.session.getRpcCall());
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(0);
        this.session.setDestBuf(bufRef);

        final FutureImpl<Message> future = new FutureImpl<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename("data").setCount(Integer.MAX_VALUE).setOffset(0).setReadPartly(true);
        Mockito
            .when(this.rpcService.getFile(this.address, rb.build(), this.copyOpts.getTimeoutMs(), session.getDone()))
            .thenReturn(future);

        this.session.onRpcReturned(new Status(RaftError.EINTR, "test"), null);
        assertNotNull(this.session.getTimer());
        Thread.sleep(this.copyOpts.getRetryIntervalMs() + 100);
        assertNotNull(this.session.getRpcCall());
        assertSame(future, this.session.getRpcCall());
        assertNull(this.session.getTimer());
    }

    private void sendNextRpc(int maxCount) {
        assertNull(this.session.getRpcCall());
        final FutureImpl<Message> future = new FutureImpl<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename("data").setCount(maxCount).setOffset(0).setReadPartly(true);
        Mockito
            .when(this.rpcService.getFile(this.address, rb.build(), this.copyOpts.getTimeoutMs(), session.getDone()))
            .thenReturn(future);
        this.session.sendNextRpc();
        assertNotNull(this.session.getRpcCall());
        assertSame(future, this.session.getRpcCall());
    }
}
