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
package com.alipay.sofa.jraft.rpc.impl;

import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.alipay.sofa.jraft.util.Requires;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

/**
 * Helper to create error response for GRPC implementation.
 *
 * @author jiachun.fjc
 */
public class GrpcResponseFactory implements RpcResponseFactory {

    @Override
    public Message newResponse(final Message parent, final int code, final String fmt, final Object... args) {
        final RpcRequests.ErrorResponse.Builder eBuilder = RpcRequests.ErrorResponse.newBuilder();
        eBuilder.setErrorCode(code);
        if (fmt != null) {
            eBuilder.setErrorMsg(String.format(fmt, args));
        }

        if (parent == null || parent instanceof RpcRequests.ErrorResponse) {
            return eBuilder.build();
        }

        final Descriptors.FieldDescriptor errFd = parent //
            .getDescriptorForType() //
            .findFieldByNumber(ERROR_RESPONSE_NUM);
        Requires.requireNonNull(errFd, "errFd");
        final Message.Builder builder = parent.toBuilder();
        for (final Descriptors.FieldDescriptor fd : parent.getDescriptorForType().getFields()) {
            builder.setField(fd, parent.getField(fd));
        }
        return builder //
            .setField(errFd, eBuilder.build()) //
            .build();
    }
}
