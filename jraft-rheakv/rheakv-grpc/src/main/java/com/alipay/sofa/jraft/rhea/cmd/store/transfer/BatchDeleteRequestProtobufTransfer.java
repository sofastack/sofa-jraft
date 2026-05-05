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

import com.alipay.sofa.jraft.rhea.cmd.store.BatchDeleteRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class BatchDeleteRequestProtobufTransfer
                                               implements
                                               GrpcSerializationTransfer<BatchDeleteRequest, RheakvRpc.BatchDeleteRequest> {

    @Override
    public BatchDeleteRequest protoBufTransJavaBean(final RheakvRpc.BatchDeleteRequest batchDeleteRequest) {
        final BatchDeleteRequest request = new BatchDeleteRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, batchDeleteRequest.getBaseRequest());
        final List<byte[]> keys = new ArrayList<>();
        batchDeleteRequest.getKeysList().forEach(key -> keys.add(key.toByteArray()));
        return request;
    }

    @Override
    public RheakvRpc.BatchDeleteRequest javaBeanTransProtobufBean(final BatchDeleteRequest batchDeleteRequest) {
        RheakvRpc.BatchDeleteRequest.Builder builder = RheakvRpc.BatchDeleteRequest.newBuilder()
                .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(batchDeleteRequest));
        batchDeleteRequest.getKeys().forEach(key -> builder.addKeys(ByteString.copyFrom(key)));
        return builder.build();
    }
}
