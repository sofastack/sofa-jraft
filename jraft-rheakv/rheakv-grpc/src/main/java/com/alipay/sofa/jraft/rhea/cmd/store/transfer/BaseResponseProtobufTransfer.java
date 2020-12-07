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

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class BaseResponseProtobufTransfer {

    /**
     * @param baseResponse
     * @param protoBaseResponse
     */
    public static void protoBufTransJavaBean(final BaseResponse baseResponse,
                                             final RheakvRpc.BaseResponse protoBaseResponse) throws CodecException {
        baseResponse.setRegionEpoch(new RegionEpoch(protoBaseResponse.getRegionEpoch().getConfVer(), protoBaseResponse
            .getRegionEpoch().getVersion()));
        baseResponse.setRegionId(protoBaseResponse.getRegionId());
        if (!protoBaseResponse.getError().isEmpty()) {
            baseResponse.setError(SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                protoBaseResponse.getError().toByteArray(), Errors.class.getName()));
        }
    }

    /**
     * @param baseResponse
     * @return
     */
    public static RheakvRpc.BaseResponse javaBeanTransProtobufBean(final BaseResponse baseResponse)
                                                                                                   throws CodecException {
        final RheakvRpc.BaseResponse.Builder builder = RheakvRpc.BaseResponse
            .newBuilder()
            .setRegionId(baseResponse.getRegionId())
            .setRegionEpoch(
                RheakvRpc.RegionEpoch.newBuilder().setConfVer(baseResponse.getRegionEpoch().getConfVer())
                    .setVersion(baseResponse.getRegionEpoch().getVersion()).build());
        if (baseResponse.getError() != null) {
            builder.setError(ByteString.copyFrom(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                baseResponse.getError())));
        }
        return builder.build();
    }
}
