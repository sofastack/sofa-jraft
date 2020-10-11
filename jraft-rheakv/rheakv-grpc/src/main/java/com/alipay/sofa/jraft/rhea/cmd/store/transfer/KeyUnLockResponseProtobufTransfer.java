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
import com.alipay.sofa.jraft.rhea.cmd.store.KeyUnlockResponse;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GRpcSerializationTransfer;

/**
 * @Author: baozi
 * @Date: 2020/9/26 20:10
 */
public class KeyUnLockResponseProtobufTransfer implements
                                              GRpcSerializationTransfer<KeyUnlockResponse, RheakvRpc.KeyUnlockResponse> {

    private final LockOwnerProtobufTransfer lockOwnerProtobufTransfer = new LockOwnerProtobufTransfer();

    @Override
    public KeyUnlockResponse protoBufTransJavaBean(final RheakvRpc.KeyUnlockResponse keyUnlockResponse)
                                                                                                       throws CodecException {
        final KeyUnlockResponse response = new KeyUnlockResponse();
        BaseResponseProtobufTransfer.protoBufTransJavaBean(response, keyUnlockResponse.getBaseResponse());
        response.setValue(lockOwnerProtobufTransfer.protoBufTransJavaBean(keyUnlockResponse.getValue()));
        return response;
    }

    @Override
    public RheakvRpc.KeyUnlockResponse javaBeanTransProtobufBean(final KeyUnlockResponse keyUnlockResponse)
                                                                                                           throws CodecException {
        final RheakvRpc.BaseResponse baseResponse = BaseResponseProtobufTransfer
            .javaBeanTransProtobufBean(keyUnlockResponse);
        final RheakvRpc.KeyUnlockResponse.Builder response = RheakvRpc.KeyUnlockResponse.newBuilder().setBaseResponse(
            baseResponse);
        response.setValue(lockOwnerProtobufTransfer.javaBeanTransProtobufBean(keyUnlockResponse.getValue()));
        return response.build();
    }

}
