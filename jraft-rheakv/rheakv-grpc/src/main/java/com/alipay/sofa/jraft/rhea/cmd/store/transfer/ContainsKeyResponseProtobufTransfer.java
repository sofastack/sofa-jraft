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
import com.alipay.sofa.jraft.rhea.cmd.store.ContainsKeyResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;

/**
 * @author: baozi
 */
public class ContainsKeyResponseProtobufTransfer
                                                implements
                                                GrpcSerializationTransfer<ContainsKeyResponse, RheakvRpc.ContainsKeyResponse> {

    @Override
    public ContainsKeyResponse protoBufTransJavaBean(RheakvRpc.ContainsKeyResponse containsKeyResponse)
                                                                                                       throws CodecException {
        final ContainsKeyResponse response = new ContainsKeyResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, containsKeyResponse.getBaseResponse());
        response.setValue(containsKeyResponse.getValue());
        return response;
    }

    @Override
    public RheakvRpc.ContainsKeyResponse javaBeanTransProtobufBean(ContainsKeyResponse containsKeyResponse)
                                                                                                           throws CodecException {
        final RheakvRpc.BaseResponse baseResponse = BaseResponseProtobufTransfer
            .javaBeanTransProtobufBean(containsKeyResponse);
        final RheakvRpc.ContainsKeyResponse.Builder response = RheakvRpc.ContainsKeyResponse.newBuilder()
            .setBaseResponse(baseResponse);
        response.setValue(containsKeyResponse.getValue());
        return response.build();
    }
}
