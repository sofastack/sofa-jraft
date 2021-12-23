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
package com.alipay.sofa.jraft.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.ReadIndexState;
import com.alipay.sofa.jraft.entity.ReadIndexStatus;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyServiceOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.Bytes;
import com.alipay.sofa.jraft.util.Utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class ReadOnlyServiceTest {

    private ReadOnlyServiceImpl readOnlyServiceImpl;

    @Mock
    private NodeImpl            node;

    @Mock
    private FSMCaller           fsmCaller;

    @Before
    public void setup() {
        this.readOnlyServiceImpl = new ReadOnlyServiceImpl();
        final ReadOnlyServiceOptions opts = new ReadOnlyServiceOptions();
        opts.setFsmCaller(this.fsmCaller);
        opts.setNode(this.node);
        opts.setRaftOptions(new RaftOptions());
        Mockito.when(this.node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        Mockito.when(this.node.getGroupId()).thenReturn("test");
        Mockito.when(this.node.getServerId()).thenReturn(new PeerId("localhost:8081", 0));
        assertTrue(this.readOnlyServiceImpl.init(opts));
    }

    @After
    public void teardown() throws Exception {
        this.readOnlyServiceImpl.shutdown();
        this.readOnlyServiceImpl.join();
    }

    @Test
    public void testAddRequest() throws Exception {
        final byte[] requestContext = TestUtils.getRandomBytes();
        this.readOnlyServiceImpl.addRequest(requestContext, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {

            }
        });
        this.readOnlyServiceImpl.flush();
        Mockito.verify(this.node).handleReadIndexRequest(Mockito.argThat(new ArgumentMatcher<ReadIndexRequest>() {

            @Override
            public boolean matches(final Object argument) {
                if (argument instanceof ReadIndexRequest) {
                    final ReadIndexRequest req = (ReadIndexRequest) argument;
                    return req.getGroupId().equals("test") && req.getServerId().equals("localhost:8081:0")
                           && req.getEntriesCount() == 1
                           && Arrays.equals(requestContext, req.getEntries(0).toByteArray());
                }
                return false;
            }

        }), Mockito.any());
    }

    @Test
    public void testAddRequestOnResponsePending() throws Exception {
        final byte[] requestContext = TestUtils.getRandomBytes();
        final CountDownLatch latch = new CountDownLatch(1);
        this.readOnlyServiceImpl.addRequest(requestContext, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertTrue(status.isOk());
                assertEquals(index, 1);
                assertArrayEquals(reqCtx, requestContext);
                latch.countDown();
            }
        });
        this.readOnlyServiceImpl.flush();

        final ArgumentCaptor<RpcResponseClosure> closureCaptor = ArgumentCaptor.forClass(RpcResponseClosure.class);

        Mockito.verify(this.node).handleReadIndexRequest(Mockito.argThat(new ArgumentMatcher<ReadIndexRequest>() {

            @Override
            public boolean matches(final Object argument) {
                if (argument instanceof ReadIndexRequest) {
                    final ReadIndexRequest req = (ReadIndexRequest) argument;
                    return req.getGroupId().equals("test") && req.getServerId().equals("localhost:8081:0")
                           && req.getEntriesCount() == 1
                           && Arrays.equals(requestContext, req.getEntries(0).toByteArray());
                }
                return false;
            }

        }), closureCaptor.capture());

        final RpcResponseClosure closure = closureCaptor.getValue();

        assertNotNull(closure);

        closure.setResponse(ReadIndexResponse.newBuilder().setIndex(1).setSuccess(true).build());
        assertTrue(this.readOnlyServiceImpl.getPendingNotifyStatus().isEmpty());
        closure.run(Status.OK());
        assertEquals(this.readOnlyServiceImpl.getPendingNotifyStatus().size(), 1);
        this.readOnlyServiceImpl.onApplied(2);
        latch.await();
    }

    @Test
    public void testAddRequestOnResponseFailure() throws Exception {
        Mockito.when(this.fsmCaller.getLastAppliedIndex()).thenReturn(2L);

        final byte[] requestContext = TestUtils.getRandomBytes();
        final CountDownLatch latch = new CountDownLatch(1);
        this.readOnlyServiceImpl.addRequest(requestContext, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertFalse(status.isOk());
                assertEquals(index, -1);
                assertArrayEquals(reqCtx, requestContext);
                latch.countDown();
            }
        });
        this.readOnlyServiceImpl.flush();

        final ArgumentCaptor<RpcResponseClosure> closureCaptor = ArgumentCaptor.forClass(RpcResponseClosure.class);

        Mockito.verify(this.node).handleReadIndexRequest(Mockito.argThat(new ArgumentMatcher<ReadIndexRequest>() {

            @Override
            public boolean matches(final Object argument) {
                if (argument instanceof ReadIndexRequest) {
                    final ReadIndexRequest req = (ReadIndexRequest) argument;
                    return req.getGroupId().equals("test") && req.getServerId().equals("localhost:8081:0")
                           && req.getEntriesCount() == 1
                           && Arrays.equals(requestContext, req.getEntries(0).toByteArray());
                }
                return false;
            }

        }), closureCaptor.capture());

        final RpcResponseClosure closure = closureCaptor.getValue();

        assertNotNull(closure);

        closure.setResponse(ReadIndexResponse.newBuilder().setIndex(1).setSuccess(true).build());
        closure.run(new Status(-1, "test"));
        latch.await();
    }

    @Test
    public void testAddRequestOnResponseSuccess() throws Exception {

        Mockito.when(this.fsmCaller.getLastAppliedIndex()).thenReturn(2L);

        final byte[] requestContext = TestUtils.getRandomBytes();
        final CountDownLatch latch = new CountDownLatch(1);
        this.readOnlyServiceImpl.addRequest(requestContext, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertTrue(status.isOk());
                assertEquals(index, 1);
                assertArrayEquals(reqCtx, requestContext);
                latch.countDown();
            }
        });
        this.readOnlyServiceImpl.flush();

        final ArgumentCaptor<RpcResponseClosure> closureCaptor = ArgumentCaptor.forClass(RpcResponseClosure.class);

        Mockito.verify(this.node).handleReadIndexRequest(Mockito.argThat(new ArgumentMatcher<ReadIndexRequest>() {

            @Override
            public boolean matches(final Object argument) {
                if (argument instanceof ReadIndexRequest) {
                    final ReadIndexRequest req = (ReadIndexRequest) argument;
                    return req.getGroupId().equals("test") && req.getServerId().equals("localhost:8081:0")
                           && req.getEntriesCount() == 1
                           && Arrays.equals(requestContext, req.getEntries(0).toByteArray());
                }
                return false;
            }

        }), closureCaptor.capture());

        final RpcResponseClosure closure = closureCaptor.getValue();

        assertNotNull(closure);

        closure.setResponse(ReadIndexResponse.newBuilder().setIndex(1).setSuccess(true).build());
        closure.run(Status.OK());
        latch.await();
    }

    @Test
    public void testOnApplied() throws Exception {
        final ArrayList<ReadIndexState> states = new ArrayList<>();
        final byte[] reqContext = TestUtils.getRandomBytes();
        final CountDownLatch latch = new CountDownLatch(1);
        final ReadIndexState state = new ReadIndexState(new Bytes(reqContext), new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertTrue(status.isOk());
                assertEquals(index, 1);
                assertArrayEquals(reqCtx, reqContext);
                latch.countDown();
            }
        }, Utils.monotonicMs());
        state.setIndex(1);
        states.add(state);
        final ReadIndexStatus readIndexStatus = new ReadIndexStatus(states, null, 1);
        this.readOnlyServiceImpl.getPendingNotifyStatus().put(1L, Arrays.asList(readIndexStatus));

        this.readOnlyServiceImpl.onApplied(2);
        latch.await();
        assertTrue(this.readOnlyServiceImpl.getPendingNotifyStatus().isEmpty());
    }

    @Test
    public void testOverMaxReadIndexLag() throws Exception {
        Mockito.when(this.fsmCaller.getLastAppliedIndex()).thenReturn(1L);
        readOnlyServiceImpl.getRaftOptions().setMaxReadIndexLag(50);

        final byte[] requestContext = TestUtils.getRandomBytes();
        final CountDownLatch latch = new CountDownLatch(1);
        final String errMsg = "Fail to run ReadIndex task, the gap of current node's apply index between leader's commit index over maxReadIndexLag";
        this.readOnlyServiceImpl.addRequest(requestContext, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                assertFalse(status.isOk());
                assertEquals(status.getErrorMsg(), errMsg);
                assertEquals(index, -1);
                assertArrayEquals(reqCtx, requestContext);
                latch.countDown();
            }
        });
        this.readOnlyServiceImpl.flush();

        final ArgumentCaptor<RpcResponseClosure> closureCaptor = ArgumentCaptor.forClass(RpcResponseClosure.class);

        Mockito.verify(this.node).handleReadIndexRequest(Mockito.argThat(new ArgumentMatcher<ReadIndexRequest>() {

            @Override
            public boolean matches(final Object argument) {
                if (argument instanceof ReadIndexRequest) {
                    final ReadIndexRequest req = (ReadIndexRequest) argument;
                    return req.getGroupId().equals("test") && req.getServerId().equals("localhost:8081:0")
                           && req.getEntriesCount() == 1
                           && Arrays.equals(requestContext, req.getEntries(0).toByteArray());
                }
                return false;
            }

        }), closureCaptor.capture());

        final RpcResponseClosure closure = closureCaptor.getValue();

        assertNotNull(closure);

        closure.setResponse(ReadIndexResponse.newBuilder().setIndex(52).setSuccess(true).build());
        closure.run(Status.OK());
        latch.await();
    }
}
