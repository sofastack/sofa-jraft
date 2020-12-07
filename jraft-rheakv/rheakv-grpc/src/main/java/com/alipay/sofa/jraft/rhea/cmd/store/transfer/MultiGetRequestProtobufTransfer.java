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

import java.util.ArrayList;
import java.util.List;

import com.alipay.sofa.jraft.rhea.cmd.store.MultiGetRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class MultiGetRequestProtobufTransfer implements
                                            GrpcSerializationTransfer<MultiGetRequest, RheakvRpc.MultiGetRequest> {

    @Override
    public MultiGetRequest protoBufTransJavaBean(final RheakvRpc.MultiGetRequest multiGetRequest) {
        final MultiGetRequest request = new MultiGetRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, multiGetRequest.getBaseRequest());
        final List<byte[]> keys = new ArrayList<>();
        multiGetRequest.getKeysList().forEach(key -> keys.add(key.toByteArray()));
        request.setKeys(keys);
        request.setReadOnlySafe(multiGetRequest.getReadOnlySafe());
        return request;
    }

    @Override
    public RheakvRpc.MultiGetRequest javaBeanTransProtobufBean(final MultiGetRequest multiGetRequest) {
        final List<ByteString> keys = new ArrayList<>();
        multiGetRequest.getKeys().forEach(key -> keys.add(ByteString.copyFrom(key)));
        return RheakvRpc.MultiGetRequest.newBuilder()
                .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(multiGetRequest))
                .addAllKeys(keys)
                .setReadOnlySafe(multiGetRequest.isReadOnlySafe())
                .build();
    }
}
