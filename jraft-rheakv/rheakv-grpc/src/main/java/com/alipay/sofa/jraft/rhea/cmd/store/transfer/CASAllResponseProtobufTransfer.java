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
import com.alipay.sofa.jraft.rhea.cmd.store.CASAllResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;

/**
 * @author: baozi
 */
public class CASAllResponseProtobufTransfer implements
                                           GrpcSerializationTransfer<CASAllResponse, RheakvRpc.CASAllResponse> {

    @Override
    public CASAllResponse protoBufTransJavaBean(final RheakvRpc.CASAllResponse casAllResponse) throws CodecException {
        final CASAllResponse response = new CASAllResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, casAllResponse.getBaseResponse());
        response.setValue(casAllResponse.getValue());
        return response;
    }

    @Override
    public RheakvRpc.CASAllResponse javaBeanTransProtobufBean(final CASAllResponse casAllResponse)
                                                                                                  throws CodecException {
        final RheakvRpc.BaseResponse baseResponse = BaseResponseProtobufTransfer
            .javaBeanTransProtobufBean(casAllResponse);
        final RheakvRpc.CASAllResponse.Builder response = RheakvRpc.CASAllResponse.newBuilder().setBaseResponse(
            baseResponse);
        response.setValue(casAllResponse.getValue() != null ? casAllResponse.getValue() : false);
        return response.build();
    }
}
