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
package com.alipay.sofa.jraft.test.atomic.server;

import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand.RequestType;

import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand.RequestType.compareAndSet;
import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand.RequestType.get;
import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand.RequestType.getSlots;
import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand.RequestType.incrementAndGet;
import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand.RequestType.set;

/**
 * RequestCommandType.
 *
 * @author zongtanghu
 */
public class RequestCommandType {

    public static byte toByte(RequestType requestType) {
        switch (requestType) {
            case get:
                return (byte) 0;
            case compareAndSet:
                return (byte) 1;
            case getSlots:
                return (byte) 2;
            case set:
                return (byte) 3;
            case incrementAndGet:
                return (byte) 4;
        }
        throw new IllegalArgumentException();
    }

    public static RequestType parseFromByte(byte b) {
        switch (b) {
            case 0:
                return get;
            case 1:
                return compareAndSet;
            case 2:
                return getSlots;
            case 3:
                return set;
            case 4:
                return incrementAndGet;
        }
        throw new IllegalArgumentException();
    }

}
