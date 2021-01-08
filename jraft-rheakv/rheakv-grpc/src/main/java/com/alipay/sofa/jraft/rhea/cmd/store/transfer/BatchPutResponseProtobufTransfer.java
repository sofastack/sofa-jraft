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
import com.alipay.sofa.jraft.rhea.cmd.store.BatchPutResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;

/**
 * @author: baozi
 */
public class BatchPutResponseProtobufTransfer implements
                                             GrpcSerializationTransfer<BatchPutResponse, RheakvRpc.BatchPutResponse> {

    @Override
    public BatchPutResponse protoBufTransJavaBean(final RheakvRpc.BatchPutResponse putResponse) throws CodecException {
        final BatchPutResponse response = new BatchPutResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, putResponse.getBaseResponse());
        response.setValue(putResponse.getValue());
        return response;
    }

    @Override
    public RheakvRpc.BatchPutResponse javaBeanTransProtobufBean(final BatchPutResponse putResponse)
                                                                                                   throws CodecException {
        final RheakvRpc.BaseResponse baseResponse = BaseResponseProtobufTransfer.javaBeanTransProtobufBean(putResponse);
        final RheakvRpc.BatchPutResponse.Builder response = RheakvRpc.BatchPutResponse.newBuilder().setBaseResponse(
            baseResponse);
        response.setValue(putResponse.getValue());
        return response.build();
    }

}
