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

import com.alipay.sofa.jraft.Closure;
import com.google.protobuf.Message;

/**
 * RPC response closure.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 5:55:01 PM 
 * @param <T>
 */
public interface RpcResponseClosure<T extends Message> extends Closure {

    /**
     * Called by request handler to set response.
     *
     * @param resp rpc response
     */
    void setResponse(T resp);
}
