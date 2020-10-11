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
import com.alipay.sofa.jraft.rhea.cmd.store.KeyLockResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;

/**
 * @Author: baozi
 * @Date: 2020/9/26 20:10
 */
public class KeyLockResponseProtobufTransfer implements
                                            GRpcSerializationTransfer<KeyLockResponse, RheakvRpc.KeyLockResponse> {

    private final LockOwnerProtobufTransfer lockOwnerProtobufTransfer = new LockOwnerProtobufTransfer();

    @Override
    public KeyLockResponse protoBufTransJavaBean(final RheakvRpc.KeyLockResponse keyLockResponse) throws CodecException {
        final KeyLockResponse response = new KeyLockResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, keyLockResponse.getBaseResponse());
        response.setValue(lockOwnerProtobufTransfer.protoBufTransJavaBean(keyLockResponse.getValue()));
        return response;
    }

    @Override
    public RheakvRpc.KeyLockResponse javaBeanTransProtobufBean(final KeyLockResponse keyLockResponse)
                                                                                                     throws CodecException {
        final RheakvRpc.BaseResponse baseResponse = BaseResponseProtobufTransfer
            .javaBeanTransProtobufBean(keyLockResponse);
        final RheakvRpc.KeyLockResponse.Builder response = RheakvRpc.KeyLockResponse.newBuilder().setBaseResponse(
            baseResponse);
        response.setValue(lockOwnerProtobufTransfer.javaBeanTransProtobufBean(keyLockResponse.getValue()));
        return response.build();
    }

}
