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
package com.alipay.sofa.jraft.rpc;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;

/**
 * Helper to create error response.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-28 4:33:50 PM
 */
public class RpcResponseFactory {

    /**
     * Creates a RPC response from status,return OK response
     * when status is null.
     *
     * @param st status with response
     * @return a response instance
     */
    public static RpcRequests.ErrorResponse newResponse(Status st) {
        if (st == null) {
            return newResponse(0, "OK");
        }
        return newResponse(st.getCode(), st.getErrorMsg());
    }

    /**
     * Creates an error response with parameters.
     *
     * @param error error with raft info
     * @param fmt   message with format string
     * @param args  arguments referenced by the format specifiers in the format
     *              string
     * @return a response instance
     */
    public static RpcRequests.ErrorResponse newResponse(RaftError error, String fmt, Object... args) {
        return newResponse(error.getNumber(), fmt, args);
    }

    /**
     * Creates an error response with parameters.
     *
     * @param code  error code with raft info
     * @param fmt   message with format string
     * @param args  arguments referenced by the format specifiers in the format
     *              string
     * @return a response instance
     */
    public static RpcRequests.ErrorResponse newResponse(int code, String fmt, Object... args) {
        RpcRequests.ErrorResponse.Builder builder = RpcRequests.ErrorResponse.newBuilder();
        builder.setErrorCode(code);
        if (fmt != null) {
            builder.setErrorMsg(String.format(fmt, args));
        }
        return builder.build();
    }
}
