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
import com.alipay.sofa.jraft.rhea.cmd.pd.GetClusterInfoResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rhea.metadata.Cluster;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @Author: baozi
 * @Date: 2020/10/16 18:33
 */
public class GetClusterInfoResponseTransfer
                                           implements
                                           GRpcSerializationTransfer<GetClusterInfoResponse, RheakvPDRpc.GetClusterInfoResponse> {

    @Override
    public GetClusterInfoResponse protoBufTransJavaBean(final RheakvPDRpc.GetClusterInfoResponse getClusterInfoResponse)
                                                                                                                        throws CodecException {
        final GetClusterInfoResponse response = new GetClusterInfoResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, getClusterInfoResponse.getBaseResponse());
        if (!getClusterInfoResponse.getValue().isEmpty()) {
            response.setCluster(SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                getClusterInfoResponse.getValue().toByteArray(), Cluster.class.getName()));
        }
        return response;
    }

    @Override
    public RheakvPDRpc.GetClusterInfoResponse javaBeanTransProtobufBean(final GetClusterInfoResponse getClusterInfoResponse)
                                                                                                                            throws CodecException {
        RheakvPDRpc.GetClusterInfoResponse.Builder builder = RheakvPDRpc.GetClusterInfoResponse.newBuilder()
            .setBaseResponse(BaseResponseProtobufTransfer.javaBeanTransProtobufBean(getClusterInfoResponse));
        if (getClusterInfoResponse.getCluster() != null) {
            byte[] clusterBytes = SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                getClusterInfoResponse.getCluster());
            builder.setValue(ByteString.copyFrom(clusterBytes));
        }
        return builder.build();
    }
}
