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
import com.alipay.sofa.jraft.rhea.cmd.pd.SetStoreInfoResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class SetStoreInfoResponseTransfer
                                         implements
                                         GrpcSerializationTransfer<SetStoreInfoResponse, RheakvPDRpc.SetStoreInfoResponse> {

    @Override
    public SetStoreInfoResponse protoBufTransJavaBean(final RheakvPDRpc.SetStoreInfoResponse setStoreInfoResponse)
                                                                                                                  throws CodecException {
        final SetStoreInfoResponse response = new SetStoreInfoResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, setStoreInfoResponse.getBaseResponse());
        if (!setStoreInfoResponse.getValue().isEmpty()) {
            response.setValue(SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                setStoreInfoResponse.getValue().toByteArray(), Store.class.getName()));
        }
        return response;
    }

    @Override
    public RheakvPDRpc.SetStoreInfoResponse javaBeanTransProtobufBean(final SetStoreInfoResponse setStoreInfoResponse)
                                                                                                                      throws CodecException {
        RheakvPDRpc.SetStoreInfoResponse.Builder builder = RheakvPDRpc.SetStoreInfoResponse.newBuilder()
            .setBaseResponse(BaseResponseProtobufTransfer.javaBeanTransProtobufBean(setStoreInfoResponse));
        if (setStoreInfoResponse.getValue() != null) {
            byte[] clusterBytes = SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                setStoreInfoResponse.getValue());
            builder.setValue(ByteString.copyFrom(clusterBytes));
        }
        return builder.build();
    }
}
