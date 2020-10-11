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

import com.alipay.remoting.exception.CodecException;
import com.alipay.sofa.jraft.rhea.cmd.store.ScanResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @Author: baozi
 * @Date: 2020/9/26 20:10
 */
public class ScanResponseProtobufTransfer implements GRpcSerializationTransfer<ScanResponse, RheakvRpc.ScanResponse> {

    @Override
    public ScanResponse protoBufTransJavaBean(final RheakvRpc.ScanResponse scanResponse) throws CodecException {
        final ScanResponse response = new ScanResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, scanResponse.getBaseResponse());
        if (scanResponse.getValueList() != null) {
            List<KVEntry> kvEntries = new ArrayList<>();
            scanResponse.getValueList().forEach(kvEntry -> kvEntries.add(new KVEntry(kvEntry.getKey().toByteArray(), kvEntry.getValue().toByteArray())));
            response.setValue(kvEntries);
        }
        return response;
    }

    @Override
    public RheakvRpc.ScanResponse javaBeanTransProtobufBean(final ScanResponse scanResponse) throws CodecException {
        final RheakvRpc.BaseResponse baseResponse = BaseResponseProtobufTransfer.javaBeanTransProtobufBean(scanResponse);
        final RheakvRpc.ScanResponse.Builder response = RheakvRpc.ScanResponse.newBuilder().setBaseResponse(baseResponse);
        if (scanResponse.getValue() != null) {
            scanResponse.getValue().forEach(kvEntry -> response.addValue(RheakvRpc.KVEntry.newBuilder()
                    .setKey(ByteString.copyFrom(kvEntry.getKey()))
                    .setValue(ByteString.copyFrom(kvEntry.getValue()))
                    .build()));
        }
        return response.build();
    }
}
