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
package com.alipay.sofa.jraft.test;

import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.google.protobuf.Message;

/**
 * mock alipay remoting async context
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-19 6:05:46 PM
 */
public class MockAsyncContext implements RpcContext {
    private Object responseObject;

    public Object getResponseObject() {
        return this.responseObject;
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> T as(Class<T> t) {
        return (T) this.responseObject;
    }

    public void setResponseObject(Object responseObject) {
        this.responseObject = responseObject;
    }

    @Override
    public void sendResponse(Object responseObject) {
        this.responseObject = responseObject;

    }

    @Override
    public Connection getConnection() {
        return null;
    }

    @Override
    public String getRemoteAddress() {
        return "localhost:12345";
    }

}
