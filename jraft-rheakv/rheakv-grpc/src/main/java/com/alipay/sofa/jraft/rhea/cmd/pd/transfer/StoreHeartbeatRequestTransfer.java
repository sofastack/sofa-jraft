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
package com.alipay.sofa.jraft.rhea.cmd.pd.transfer;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.metadata.StoreState;
import com.alipay.sofa.jraft.rhea.metadata.StoreStats;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @Author: baozi
 * @Date: 2020/10/16 18:33
 */
public class StoreHeartbeatRequestTransfer
                                          implements
                                          GRpcSerializationTransfer<StoreHeartbeatRequest, RheakvPDRpc.StoreHeartbeatRequest> {

    @Override
    public StoreHeartbeatRequest protoBufTransJavaBean(final RheakvPDRpc.StoreHeartbeatRequest storeHeartbeatRequest)
                                                                                                                     throws CodecException {
        final StoreHeartbeatRequest request = new StoreHeartbeatRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, storeHeartbeatRequest.getBaseRequest());
        if (!storeHeartbeatRequest.getStats().isEmpty()) {
            request.setStats(SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                storeHeartbeatRequest.getStats().toByteArray(), StoreStats.class.getName()));
        }
        return request;
    }

    @Override
    public RheakvPDRpc.StoreHeartbeatRequest javaBeanTransProtobufBean(final StoreHeartbeatRequest storeHeartbeatRequest)
                                                                                                                         throws CodecException {
        RheakvPDRpc.StoreHeartbeatRequest.Builder builder = RheakvPDRpc.StoreHeartbeatRequest.newBuilder()
            .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(storeHeartbeatRequest));
        if (storeHeartbeatRequest.getStats() != null) {
            byte[] endpointBytes = SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                storeHeartbeatRequest.getStats());
            builder.setStats(ByteString.copyFrom(endpointBytes));
        }
        return builder.build();
    }
}
