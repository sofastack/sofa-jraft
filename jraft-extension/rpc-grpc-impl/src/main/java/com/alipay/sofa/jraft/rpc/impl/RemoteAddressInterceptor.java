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

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import org.apache.commons.lang.StringUtils;

/**
 * GRPC server interceptor to trace remote address.
 *
 * @author nicholas.jxf
 */
public class RemoteAddressInterceptor implements ServerInterceptor {

    static final String REMOTE_ADDRESS = "remote-address";

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
                                                                 final Metadata metadata,
                                                                 final ServerCallHandler<ReqT, RespT> serverCallHandler) {
        final Context context = Context.current().withValue(Context.key(REMOTE_ADDRESS),
            parseSocketAddress(serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)));
        return Contexts.interceptCall(context, serverCall, metadata, serverCallHandler);
    }

    /**
     * Parse socket address to host:ip pattern.
     *
     * @param socketAddress socket address
     * @return remote address
     */
    public static String parseSocketAddress(final Object socketAddress) {
        try {
            return socketAddress.toString().trim().split("/")[0];
        } catch (final Exception e) {
            return StringUtils.EMPTY;
        }
    }
}
