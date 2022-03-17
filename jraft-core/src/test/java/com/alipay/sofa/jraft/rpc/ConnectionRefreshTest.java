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

import org.junit.Ignore;
import org.junit.Test;

import com.alipay.sofa.jraft.rpc.impl.PingRequestProcessor;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

/**
 *
 * @author jiachun.fjc
 */
public class ConnectionRefreshTest {

    @Ignore
    @Test
    public void simulation() throws InterruptedException {
        ProtobufMsgFactory.load();

        final RpcServer server = RpcFactoryHelper.rpcFactory().createRpcServer(new Endpoint("127.0.0.1", 19991));
        server.registerProcessor(new PingRequestProcessor());
        server.init(null);

        final Endpoint target = new Endpoint("my.test.host1.com", 19991);

        final RpcClient client = RpcFactoryHelper.rpcFactory().createRpcClient();
        client.init(null);

        final RpcRequests.PingRequest req = RpcRequests.PingRequest.newBuilder() //
            .setSendTimestamp(System.currentTimeMillis()) //
            .build();

        for (int i = 0; i < 1000; i++) {
            try {
                final Object resp = client.invokeSync(target, req, 3000);
                System.out.println(resp);
            } catch (final Exception e) {
                e.printStackTrace();
            }
            Thread.sleep(1000);
        }
    }
}
