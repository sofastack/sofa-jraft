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
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.ByteString;

/**
 * @Author: baozi
 * @Date: 2020/10/16 18:33
 */
public class GetStoreIdRequestTransfer implements
                                      GRpcSerializationTransfer<GetStoreIdRequest, RheakvPDRpc.GetStoreIdRequest> {

    @Override
    public GetStoreIdRequest protoBufTransJavaBean(final RheakvPDRpc.GetStoreIdRequest getStoreIdRequest)
                                                                                                         throws CodecException {
        final GetStoreIdRequest request = new GetStoreIdRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, getStoreIdRequest.getBaseRequest());
        if (!getStoreIdRequest.getEndpoint().isEmpty()) {
            request.setEndpoint(SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                getStoreIdRequest.getEndpoint().toByteArray(), Endpoint.class.getName()));
        }
        return request;
    }

    @Override
    public RheakvPDRpc.GetStoreIdRequest javaBeanTransProtobufBean(final GetStoreIdRequest getStoreIdRequest)
                                                                                                             throws CodecException {
        RheakvPDRpc.GetStoreIdRequest.Builder builder = RheakvPDRpc.GetStoreIdRequest.newBuilder().setBaseRequest(
            BaseRequestProtobufTransfer.javaBeanTransProtobufBean(getStoreIdRequest));
        if (getStoreIdRequest.getEndpoint() != null) {
            byte[] endpointBytes = SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                getStoreIdRequest.getEndpoint());
            builder.setEndpoint(ByteString.copyFrom(endpointBytes));
        }
        return builder.build();
    }
}
