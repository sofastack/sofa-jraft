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
package com.alipay.sofa.jraft.rhea.cmd.store.transfer;

import com.alipay.sofa.jraft.rhea.cmd.store.CompareAndPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class CompareAndPutRequestProtobufTransfer
                                                 implements
                                                 GrpcSerializationTransfer<CompareAndPutRequest, RheakvRpc.CompareAndPutRequest> {

    @Override
    public CompareAndPutRequest protoBufTransJavaBean(final RheakvRpc.CompareAndPutRequest compareAndPutRequest) {
        final CompareAndPutRequest request = new CompareAndPutRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, compareAndPutRequest.getBaseRequest());
        request.setKey(compareAndPutRequest.getKey().toByteArray());
        request.setExpect(compareAndPutRequest.getExpect().toByteArray());
        request.setUpdate(compareAndPutRequest.getUpdate().toByteArray());
        return request;
    }

    @Override
    public RheakvRpc.CompareAndPutRequest javaBeanTransProtobufBean(final CompareAndPutRequest getRequest) {
        return RheakvRpc.CompareAndPutRequest.newBuilder()
            .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(getRequest))
            .setKey(ByteString.copyFrom(getRequest.getKey())).setExpect(ByteString.copyFrom(getRequest.getExpect()))
            .setUpdate(ByteString.copyFrom(getRequest.getUpdate())).build();
    }
}
