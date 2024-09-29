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

import org.junit.Test;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RpcRequests.ErrorResponse;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

import static org.junit.Assert.assertEquals;

public class RpcResponseFactoryTest {
    @Test
    public void testNewResponseWithNull() {
        ErrorResponse response = (ErrorResponse) RpcFactoryHelper.responseFactory().newResponse(null, null);
        assertEquals(response.getErrorCode(), 0);
        assertEquals(response.getErrorMsg(), "OK");
    }

    @Test
    public void testNewResponseFromStatus() {
        ErrorResponse response = (ErrorResponse) RpcFactoryHelper.responseFactory().newResponse(null, Status.OK());
        assertEquals(response.getErrorCode(), 0);
        assertEquals(response.getErrorMsg(), "");
    }

    @Test
    public void testNewResponseWithErrorStatus() {
        ErrorResponse response = (ErrorResponse) RpcFactoryHelper.responseFactory().newResponse(null,
            new Status(300, "test"));
        assertEquals(response.getErrorCode(), 300);
        assertEquals(response.getErrorMsg(), "test");
    }

    @Test
    public void testNewResponseWithVaridicNumberArgs() {
        ErrorResponse response = (ErrorResponse) RpcFactoryHelper.responseFactory().newResponse(null, 300,
            "hello %s %d", "world", 99);
        assertEquals(response.getErrorCode(), 300);
        assertEquals(response.getErrorMsg(), "hello world 99");
    }

    @Test
    public void testNewResponseWithVaridicStringArgs() {
        ErrorResponse response = (ErrorResponse) RpcFactoryHelper.responseFactory().newResponse(null, 300,
            "hello %s %s", "world", "99");
        assertEquals(response.getErrorCode(), 300);
        assertEquals(response.getErrorMsg(), "hello world 99");
    }

    @Test
    public void testNewResponseWithArgs() {
        ErrorResponse response = (ErrorResponse) RpcFactoryHelper.responseFactory().newResponse(null, 300,
            "hello world");
        assertEquals(response.getErrorCode(), 300);
        assertEquals(response.getErrorMsg(), "hello world");
    }

    @Test
    public void testNewResponseWithRaftError() {
        ErrorResponse response = (ErrorResponse) RpcFactoryHelper.responseFactory().newResponse(null, RaftError.EAGAIN,
            "hello world");
        assertEquals(response.getErrorCode(), RaftError.EAGAIN.getNumber());
        assertEquals(response.getErrorMsg(), "hello world");
    }
}
