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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alipay.remoting.exception.CodecException;
import com.alipay.sofa.jraft.rhea.cmd.store.MultiGetResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @Author: baozi
 * @Date: 2020/9/26 20:10
 */
public class MultiGetResponseProtobufTransfer implements
                                             GRpcSerializationTransfer<MultiGetResponse, RheakvRpc.MultiGetResponse> {

    @Override
    public MultiGetResponse protoBufTransJavaBean(RheakvRpc.MultiGetResponse multiGetResponse) throws CodecException {
        final MultiGetResponse response = new MultiGetResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, multiGetResponse.getBaseResponse());
        if (multiGetResponse.getValueList() != null) {
            Map<ByteArray, byte[]> value = new HashMap<>();
            multiGetResponse.getValueList().forEach(kvEntry -> value.put(ByteArray.wrap(kvEntry.getKey().toByteArray()), kvEntry.getValue().toByteArray()));
            response.setValue(value);
        }
        return response;
    }

    @Override
    public RheakvRpc.MultiGetResponse javaBeanTransProtobufBean(MultiGetResponse multiGetResponse) throws CodecException {
        final RheakvRpc.BaseResponse baseResponse = BaseResponseProtobufTransfer.javaBeanTransProtobufBean(multiGetResponse);
        final RheakvRpc.MultiGetResponse.Builder response = RheakvRpc.MultiGetResponse.newBuilder().setBaseResponse(baseResponse);
        if (multiGetResponse.getValue() != null) {
            List<RheakvRpc.KVEntry> values = new ArrayList<>();
            multiGetResponse.getValue().forEach((key, value) -> {
                RheakvRpc.KVEntry kvEntry = RheakvRpc.KVEntry.newBuilder()
                        .setKey(ByteString.copyFrom(key.getBytes()))
                        .setValue(ByteString.copyFrom(value))
                        .build();
                values.add(kvEntry);
            });
            response.addAllValue(values);
        }
        return response.build();
    }
}
