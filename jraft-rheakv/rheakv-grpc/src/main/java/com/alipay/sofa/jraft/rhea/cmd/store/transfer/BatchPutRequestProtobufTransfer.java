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

import com.alipay.sofa.jraft.rhea.cmd.store.BatchPutRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @Author: baozi
 * @Date: 2020/9/27 20:44
 */
public class BatchPutRequestProtobufTransfer implements
                                            GRpcSerializationTransfer<BatchPutRequest, RheakvRpc.BatchPutRequest> {

    @Override
    public BatchPutRequest protoBufTransJavaBean(final RheakvRpc.BatchPutRequest batchPutRequest) {
        final BatchPutRequest request = new BatchPutRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, batchPutRequest.getBaseRequest());
        List<KVEntry> kvEntries = new ArrayList<>();
        batchPutRequest.getKvEntriesList().forEach(kvEntry -> kvEntries.add(new KVEntry(kvEntry.getKey().toByteArray(), kvEntry.getValue().toByteArray())));
        request.setKvEntries(kvEntries);
        return request;
    }

    @Override
    public RheakvRpc.BatchPutRequest javaBeanTransProtobufBean(final BatchPutRequest batchPutRequest) {
        RheakvRpc.BatchPutRequest.Builder builder = RheakvRpc.BatchPutRequest.newBuilder()
                .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(batchPutRequest));
        batchPutRequest.getKvEntries().forEach(kvEntry -> builder.addKvEntries(RheakvRpc.KVEntry.newBuilder()
                .setKey(ByteString.copyFrom(kvEntry.getKey()))
                .setValue(ByteString.copyFrom(kvEntry.getValue()))
                .build()));
        return builder.build();
    }
}
