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
import com.alipay.sofa.jraft.rhea.cmd.pd.CreateRegionIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class CreateRegionIdRequestTransfer
                                          implements
                                          GrpcSerializationTransfer<CreateRegionIdRequest, RheakvPDRpc.CreateRegionIdRequest> {

    @Override
    public CreateRegionIdRequest protoBufTransJavaBean(final RheakvPDRpc.CreateRegionIdRequest createRegionIdRequest)
                                                                                                                     throws CodecException {
        final CreateRegionIdRequest request = new CreateRegionIdRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, createRegionIdRequest.getBaseRequest());
        if (!createRegionIdRequest.getEndpoint().isEmpty()) {
            request.setEndpoint(SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                createRegionIdRequest.getEndpoint().toByteArray(), Endpoint.class.getName()));
        }
        return request;
    }

    @Override
    public RheakvPDRpc.CreateRegionIdRequest javaBeanTransProtobufBean(final CreateRegionIdRequest createRegionIdRequest)
                                                                                                                         throws CodecException {
        final RheakvPDRpc.CreateRegionIdRequest.Builder builder = RheakvPDRpc.CreateRegionIdRequest.newBuilder()
            .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(createRegionIdRequest));
        if (createRegionIdRequest.getEndpoint() != null) {
            byte[] endpointBytes = SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                createRegionIdRequest.getEndpoint());
            builder.setEndpoint(ByteString.copyFrom(endpointBytes));
        }
        return builder.build();
    }
}
