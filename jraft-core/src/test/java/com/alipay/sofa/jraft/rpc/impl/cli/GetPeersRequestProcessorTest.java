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
package com.alipay.sofa.jraft.rpc.impl.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.mockito.ArgumentCaptor;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.CliRequests.GetPeersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.GetPeersResponse;

public class GetPeersRequestProcessorTest extends AbstractCliRequestProcessorTest<GetPeersRequest> {

    @Override
    public GetPeersRequest createRequest(final String groupId, final PeerId peerId) {
        return GetPeersRequest.newBuilder(). //
            setGroupId(groupId). //
            build();
    }

    @Override
    public BaseCliRequestProcessor<GetPeersRequest> newProcessor() {
        return new GetPeersRequestProcessor(null);
    }

    @Override
    public void verify(final String interest, final Node node, final ArgumentCaptor<Closure> doneArg) {
        assertEquals(interest, GetPeersRequest.class.getName());
        assertNotNull(this.asyncContext.getResponseObject());
        assertEquals("[localhost:8081, localhost:8082, localhost:8083]", this.asyncContext.as(GetPeersResponse.class)
            .getPeersList().toString());
        assertEquals("[learner:8081, learner:8082, learner:8083]", this.asyncContext.as(GetPeersResponse.class)
            .getLearnersList().toString());
    }

}
