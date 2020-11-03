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
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.proto.RheakvPDRpc;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.google.protobuf.ByteString;

/**
 * @Author: baozi
 * @Date: 2020/9/27 20:17
 */
public class BaseResponseProtobufTransfer {

    /**
     * @param baseResponse
     * @param protoBaseResponse
     */
    public static void protoBufTransJavaBean(final BaseResponse baseResponse,
                                             final RheakvPDRpc.BaseResponse protoBaseResponse) throws CodecException {
        baseResponse.setClusterId(protoBaseResponse.getClusterId());
        if (!protoBaseResponse.getError().isEmpty()) {
            baseResponse.setError(SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                protoBaseResponse.getError().toByteArray(), Errors.class.getName()));
        }
    }

    /**
     * @param baseResponse
     * @return
     */
    public static RheakvPDRpc.BaseResponse javaBeanTransProtobufBean(final BaseResponse baseResponse)
                                                                                                     throws CodecException {
        final RheakvPDRpc.BaseResponse.Builder builder = RheakvPDRpc.BaseResponse.newBuilder().setClusterId(
            baseResponse.getClusterId());
        if (baseResponse.getError() != null) {
            builder.setError(ByteString.copyFrom(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(
                baseResponse.getError())));
        }
        return builder.build();
    }
}
