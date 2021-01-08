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
import com.alipay.sofa.jraft.rhea.cmd.pd.SetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class SetStoreInfoRequestTransfer implements
                                        GrpcSerializationTransfer<SetStoreInfoRequest, RheakvPDRpc.SetStoreInfoRequest> {

    @Override
    public SetStoreInfoRequest protoBufTransJavaBean(final RheakvPDRpc.SetStoreInfoRequest setStoreInfoRequest)
                                                                                                               throws CodecException {
        final SetStoreInfoRequest request = new SetStoreInfoRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, setStoreInfoRequest.getBaseRequest());
        if (!setStoreInfoRequest.getStore().isEmpty()) {
            request.setStore(SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                setStoreInfoRequest.getStore().toByteArray(), Store.class.getName()));
        }
        return request;
    }

    @Override
    public RheakvPDRpc.SetStoreInfoRequest javaBeanTransProtobufBean(final SetStoreInfoRequest setStoreInfoRequest)
                                                                                                                   throws CodecException {
        RheakvPDRpc.SetStoreInfoRequest.Builder builder = RheakvPDRpc.SetStoreInfoRequest.newBuilder().setBaseRequest(
            BaseRequestProtobufTransfer.javaBeanTransProtobufBean(setStoreInfoRequest));
        if (setStoreInfoRequest.getStore() != null) {
            byte[] endpointBytes = SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                setStoreInfoRequest.getStore());
            builder.setStore(ByteString.copyFrom(endpointBytes));
        }
        return builder.build();
    }
}
