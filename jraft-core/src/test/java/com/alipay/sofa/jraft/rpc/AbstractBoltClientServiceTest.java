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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.Url;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.exception.InvokeTimeoutException;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.ErrorResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.PingRequest;
import com.alipay.sofa.jraft.rpc.impl.AbstractBoltClientService;
import com.alipay.sofa.jraft.rpc.impl.core.JRaftRpcAddressParser;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;

@RunWith(value = MockitoJUnitRunner.class)
public class AbstractBoltClientServiceTest {
    static class MockBoltClientService extends AbstractBoltClientService {
        public void setRpcClient(final RpcClient rpcClient) {
            this.rpcClient = rpcClient;
        }
    }

    private RpcOptions                  rpcOptions;
    private MockBoltClientService       clientService;
    @Mock
    private RpcClient                   rpcClient;
    private final JRaftRpcAddressParser rpcAddressParser = new JRaftRpcAddressParser();
    private final Endpoint              endpoint         = new Endpoint("localhost", 8081);

    @Before
    public void setup() {
        this.rpcOptions = new RpcOptions();
        this.clientService = new MockBoltClientService();
        assertTrue(this.clientService.init(this.rpcOptions));
        this.clientService.setRpcClient(this.rpcClient);

    }

    @Test
    public void testConnect() throws Exception {
        Mockito.when(
            this.rpcClient.invokeSync(eq(this.endpoint.toString()), Mockito.any(), Mockito.any(),
                eq(this.rpcOptions.getRpcConnectTimeoutMs()))).thenReturn(RpcResponseFactory.newResponse(Status.OK()));
        assertTrue(this.clientService.connect(this.endpoint));
    }

    @Test
    public void testConnectFailure() throws Exception {
        Mockito.when(
            this.rpcClient.invokeSync(eq(this.endpoint.toString()), Mockito.any(), Mockito.any(),
                eq(this.rpcOptions.getRpcConnectTimeoutMs()))).thenReturn(
            RpcResponseFactory.newResponse(new Status(-1, "test")));
        assertFalse(this.clientService.connect(this.endpoint));
    }

    @Test
    public void testConnectException() throws Exception {
        Mockito.when(
            this.rpcClient.invokeSync(eq(this.endpoint.toString()), Mockito.any(), Mockito.any(),
                eq(this.rpcOptions.getRpcConnectTimeoutMs()))).thenThrow(new RemotingException("test"));
        assertFalse(this.clientService.connect(this.endpoint));
    }

    @Test
    public void testDisconnect() {
        this.clientService.disconnect(this.endpoint);
        Mockito.verify(this.rpcClient).closeConnection(this.endpoint.toString());
    }

    static class MockRpcResponseClosure<T extends Message> extends RpcResponseClosureAdapter<T> {

        CountDownLatch latch = new CountDownLatch(1);

        Status         status;

        @Override
        public void run(final Status status) {
            this.status = status;
            this.latch.countDown();
        }

    }

    @Test
    public void testCancel() throws Exception {
        ArgumentCaptor<InvokeCallback> callbackArg = ArgumentCaptor.forClass(InvokeCallback.class);
        PingRequest request = TestUtils.createPingRequest();

        MockRpcResponseClosure<ErrorResponse> done = new MockRpcResponseClosure<>();
        Future<Message> future = this.clientService.invokeWithDone(this.endpoint, request, done, -1);
        Url rpcUrl = this.rpcAddressParser.parse(this.endpoint.toString());
        Mockito.verify(this.rpcClient).invokeWithCallback(eq(rpcUrl), eq(request), Mockito.any(),
            callbackArg.capture(), eq(this.rpcOptions.getRpcDefaultTimeout()));
        InvokeCallback cb = callbackArg.getValue();
        assertNotNull(cb);
        assertNotNull(future);

        assertNull(done.getResponse());
        assertNull(done.status);
        assertFalse(future.isDone());

        future.cancel(true);
        ErrorResponse response = RpcResponseFactory.newResponse(Status.OK());
        cb.onResponse(response);

        // The closure should be notified with ECANCELED error code.
        done.latch.await();
        assertNotNull(done.status);
        assertEquals(RaftError.ECANCELED.getNumber(), done.status.getCode());
    }

    @Test
    public void testInvokeWithDoneOK() throws Exception {
        ArgumentCaptor<InvokeCallback> callbackArg = ArgumentCaptor.forClass(InvokeCallback.class);
        PingRequest request = TestUtils.createPingRequest();

        MockRpcResponseClosure<ErrorResponse> done = new MockRpcResponseClosure<>();
        Future<Message> future = this.clientService.invokeWithDone(this.endpoint, request, done, -1);
        Url rpcUrl = this.rpcAddressParser.parse(this.endpoint.toString());
        Mockito.verify(this.rpcClient).invokeWithCallback(eq(rpcUrl), eq(request), Mockito.any(),
            callbackArg.capture(), eq(this.rpcOptions.getRpcDefaultTimeout()));
        InvokeCallback cb = callbackArg.getValue();
        assertNotNull(cb);
        assertNotNull(future);

        assertNull(done.getResponse());
        assertNull(done.status);
        assertFalse(future.isDone());

        ErrorResponse response = RpcResponseFactory.newResponse(Status.OK());
        cb.onResponse(response);

        Message msg = future.get();
        assertNotNull(msg);
        assertTrue(msg instanceof ErrorResponse);
        assertSame(msg, response);

        done.latch.await();
        assertNotNull(done.status);
        assertEquals(0, done.status.getCode());
    }

    @Test
    public void testInvokeWithDoneException() throws Exception {
        InvokeContext invokeCtx = new InvokeContext();
        invokeCtx.put(InvokeContext.BOLT_CRC_SWITCH, false);
        ArgumentCaptor<InvokeCallback> callbackArg = ArgumentCaptor.forClass(InvokeCallback.class);
        PingRequest request = TestUtils.createPingRequest();

        Url rpcUrl = this.rpcAddressParser.parse(this.endpoint.toString());
        Mockito
            .doThrow(new RemotingException())
            .when(this.rpcClient)
            .invokeWithCallback(eq(rpcUrl), eq(request), eq(invokeCtx), callbackArg.capture(),
                eq(this.rpcOptions.getRpcDefaultTimeout()));

        MockRpcResponseClosure<ErrorResponse> done = new MockRpcResponseClosure<>();
        Future<Message> future = this.clientService.invokeWithDone(this.endpoint, request, invokeCtx, done, -1);
        InvokeCallback cb = callbackArg.getValue();
        assertNotNull(cb);
        assertNotNull(future);

        assertTrue(future.isDone());

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RemotingException);
        }

        done.latch.await();
        assertNotNull(done.status);
        assertEquals(RaftError.EINTERNAL.getNumber(), done.status.getCode());
    }

    @Test
    public void testInvokeWithDoneOnException() throws Exception {
        InvokeContext invokeCtx = new InvokeContext();
        invokeCtx.put(InvokeContext.BOLT_CRC_SWITCH, false);
        ArgumentCaptor<InvokeCallback> callbackArg = ArgumentCaptor.forClass(InvokeCallback.class);
        PingRequest request = TestUtils.createPingRequest();

        MockRpcResponseClosure<ErrorResponse> done = new MockRpcResponseClosure<>();
        Future<Message> future = this.clientService.invokeWithDone(this.endpoint, request, invokeCtx, done, -1);
        Url rpcUrl = this.rpcAddressParser.parse(this.endpoint.toString());
        Mockito.verify(this.rpcClient).invokeWithCallback(eq(rpcUrl), eq(request), eq(invokeCtx),
            callbackArg.capture(), eq(this.rpcOptions.getRpcDefaultTimeout()));
        InvokeCallback cb = callbackArg.getValue();
        assertNotNull(cb);
        assertNotNull(future);

        assertNull(done.getResponse());
        assertNull(done.status);
        assertFalse(future.isDone());

        cb.onException(new InvokeTimeoutException());

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof InvokeTimeoutException);
        }

        done.latch.await();
        assertNotNull(done.status);
        assertEquals(RaftError.ETIMEDOUT.getNumber(), done.status.getCode());
    }
}
