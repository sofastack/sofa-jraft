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
import com.alipay.sofa.jraft.rhea.cmd.store.PutIfAbsentResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class PutIfAbsentResponseProtobufTransfer
                                                implements
                                                GrpcSerializationTransfer<PutIfAbsentResponse, RheakvRpc.PutIfAbsentResponse> {

    @Override
    public PutIfAbsentResponse protoBufTransJavaBean(final RheakvRpc.PutIfAbsentResponse putIfAbsentResponse)
                                                                                                             throws CodecException {
        final PutIfAbsentResponse response = new PutIfAbsentResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, putIfAbsentResponse.getBaseResponse());
        if (!putIfAbsentResponse.getValue().isEmpty()) {
            response.setValue(putIfAbsentResponse.getValue().toByteArray());
        }
        return response;
    }

    @Override
    public RheakvRpc.PutIfAbsentResponse javaBeanTransProtobufBean(final PutIfAbsentResponse putIfAbsentResponse)
                                                                                                                 throws CodecException {
        final RheakvRpc.BaseResponse baseResponse = BaseResponseProtobufTransfer
            .javaBeanTransProtobufBean(putIfAbsentResponse);
        final RheakvRpc.PutIfAbsentResponse.Builder response = RheakvRpc.PutIfAbsentResponse.newBuilder()
            .setBaseResponse(baseResponse);
        if (putIfAbsentResponse.getValue() != null) {
            response.setValue(ByteString.copyFrom(putIfAbsentResponse.getValue()));
        }
        return response.build();
    }

}
