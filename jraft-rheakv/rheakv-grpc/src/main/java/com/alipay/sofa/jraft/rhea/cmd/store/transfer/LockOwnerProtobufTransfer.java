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

import com.alipay.sofa.jraft.rhea.cmd.store.proto.RheakvRpc;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.rpc.impl.GrpcSerializationTransfer;
import com.google.protobuf.ByteString;

/**
 * @author: baozi
 */
public class LockOwnerProtobufTransfer implements GrpcSerializationTransfer<DistributedLock.Owner, RheakvRpc.LockOwner> {

    @Override
    public DistributedLock.Owner protoBufTransJavaBean(RheakvRpc.LockOwner lockOwner) {
        byte[] context = null;
        if (!lockOwner.getContext().isEmpty()) {
            context = lockOwner.getContext().toByteArray();
        }
        DistributedLock.Owner owner = new DistributedLock.Owner(lockOwner.getId(), lockOwner.getDeadlineMillis(),
            lockOwner.getRemainingMillis(), lockOwner.getFencingToken(), lockOwner.getAcquires(), context,
            lockOwner.getSuccess());
        return owner;
    }

    @Override
    public RheakvRpc.LockOwner javaBeanTransProtobufBean(DistributedLock.Owner owner) {
        RheakvRpc.LockOwner.Builder builder = RheakvRpc.LockOwner.newBuilder().setId(owner.getId())
            .setDeadlineMillis(owner.getDeadlineMillis()).setRemainingMillis(owner.getRemainingMillis())
            .setFencingToken(owner.getFencingToken()).setAcquires(owner.getAcquires()).setSuccess(owner.isSuccess());
        if (owner.getContext() != null) {
            builder.setContext(ByteString.copyFrom(owner.getContext()));
        }
        return builder.build();
    }
}
