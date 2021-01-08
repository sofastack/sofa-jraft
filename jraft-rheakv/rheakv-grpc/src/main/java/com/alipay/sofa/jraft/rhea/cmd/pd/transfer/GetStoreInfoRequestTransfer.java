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
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class GetStoreInfoRequestTransfer implements
                                        GrpcSerializationTransfer<GetStoreInfoRequest, RheakvPDRpc.GetStoreInfoRequest> {

    @Override
    public GetStoreInfoRequest protoBufTransJavaBean(final RheakvPDRpc.GetStoreInfoRequest getStoreInfoRequest)
                                                                                                               throws CodecException {
        final GetStoreInfoRequest request = new GetStoreInfoRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, getStoreInfoRequest.getBaseRequest());
        if (!getStoreInfoRequest.getEndpoint().isEmpty()) {
            request.setEndpoint(SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                getStoreInfoRequest.getEndpoint().toByteArray(), Endpoint.class.getName()));
        }
        return request;
    }

    @Override
    public RheakvPDRpc.GetStoreInfoRequest javaBeanTransProtobufBean(final GetStoreInfoRequest getStoreInfoRequest)
                                                                                                                   throws CodecException {
        RheakvPDRpc.GetStoreInfoRequest.Builder builder = RheakvPDRpc.GetStoreInfoRequest.newBuilder().setBaseRequest(
            BaseRequestProtobufTransfer.javaBeanTransProtobufBean(getStoreInfoRequest));
        if (getStoreInfoRequest.getEndpoint() != null) {
            byte[] endpointBytes = SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                getStoreInfoRequest.getEndpoint());
            builder.setEndpoint(ByteString.copyFrom(endpointBytes));
        }
        return builder.build();
    }
}
