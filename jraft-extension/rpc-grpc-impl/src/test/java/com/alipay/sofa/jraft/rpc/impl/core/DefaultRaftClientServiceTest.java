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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.util.Endpoint;

@RunWith(value = MockitoJUnitRunner.class)
public class DefaultRaftClientServiceTest {
    private DefaultRaftClientService clientService;
    @Mock
    private ReplicatorGroup          rgGroup;

    private final Endpoint           endpoint = new Endpoint("localhost", 8081);

    @Before
    public void setup() {
        this.clientService = new DefaultRaftClientService(this.rgGroup);
        this.clientService.init(new NodeOptions());
    }

    @Test
    public void testPreVote() {
        this.clientService.preVote(this.endpoint, RequestVoteRequest.newBuilder(). //
            setGroupId("test"). //
            setLastLogIndex(1). //
            setLastLogTerm(1). //
            setPeerId("localhost:1010"). //
            setTerm(1).setServerId("localhost:1011").setPreVote(true).build(), null);
    }

}
