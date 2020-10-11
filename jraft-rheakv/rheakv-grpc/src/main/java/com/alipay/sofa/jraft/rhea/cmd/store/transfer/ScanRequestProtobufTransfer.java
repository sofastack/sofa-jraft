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

import com.alipay.sofa.jraft.rhea.cmd.store.ScanRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @Author: baozi
 * @Date: 2020/9/27 20:44
 */
public class ScanRequestProtobufTransfer implements GRpcSerializationTransfer<ScanRequest, RheakvRpc.ScanRequest> {

    @Override
    public ScanRequest protoBufTransJavaBean(final RheakvRpc.ScanRequest scanRequest) {
        final ScanRequest request = new ScanRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, scanRequest.getBaseRequest());
        request.setStartKey(scanRequest.getStartKey().toByteArray());
        request.setEndKey(scanRequest.getEndKey().toByteArray());
        request.setLimit(scanRequest.getLimit());
        request.setReadOnlySafe(scanRequest.getReadOnlySafe());
        request.setReturnValue(scanRequest.getReturnValue());
        request.setReverse(scanRequest.getReverse());
        return request;
    }

    @Override
    public RheakvRpc.ScanRequest javaBeanTransProtobufBean(final ScanRequest scanRequest) {
        return RheakvRpc.ScanRequest.newBuilder()
            .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(scanRequest))
            .setStartKey(ByteString.copyFrom(scanRequest.getStartKey()))
            .setEndKey(ByteString.copyFrom(scanRequest.getEndKey())).setLimit(scanRequest.getLimit())
            .setReadOnlySafe(scanRequest.isReadOnlySafe()).setReturnValue(scanRequest.isReturnValue())
            .setReverse(scanRequest.isReverse()).build();
    }
}
