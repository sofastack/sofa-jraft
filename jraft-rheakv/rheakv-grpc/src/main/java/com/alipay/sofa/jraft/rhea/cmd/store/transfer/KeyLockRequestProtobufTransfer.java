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

import com.alipay.sofa.jraft.rhea.cmd.store.KeyLockRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class KeyLockRequestProtobufTransfer implements
                                           GrpcSerializationTransfer<KeyLockRequest, RheakvRpc.KeyLockRequest> {

    private final LockAcquirerProtobufTransfer lockAcquirerProtobufTransfer = new LockAcquirerProtobufTransfer();

    @Override
    public KeyLockRequest protoBufTransJavaBean(final RheakvRpc.KeyLockRequest keyLockRequest) {
        final KeyLockRequest request = new KeyLockRequest();
        BaseRequestProtobufTransfer.protoBufTransJavaBean(request, keyLockRequest.getBaseRequest());
        request.setKey(keyLockRequest.getKey().toByteArray());
        request.setKeepLease(keyLockRequest.getKeepLease());
        request.setAcquirer(lockAcquirerProtobufTransfer.protoBufTransJavaBean(keyLockRequest.getAcquirer()));
        return request;
    }

    @Override
    public RheakvRpc.KeyLockRequest javaBeanTransProtobufBean(final KeyLockRequest keyLockRequest) {
        return RheakvRpc.KeyLockRequest.newBuilder()
            .setBaseRequest(BaseRequestProtobufTransfer.javaBeanTransProtobufBean(keyLockRequest))
            .setKey(ByteString.copyFrom(keyLockRequest.getKey())).setKeepLease(keyLockRequest.isKeepLease())
            .setAcquirer(lockAcquirerProtobufTransfer.javaBeanTransProtobufBean(keyLockRequest.getAcquirer())).build();
    }
}
