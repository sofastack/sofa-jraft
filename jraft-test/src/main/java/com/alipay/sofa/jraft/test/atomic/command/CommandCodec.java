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
package com.alipay.sofa.jraft.test.atomic.command;

import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetCommand;
import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.SetCommand;
import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.IncrementAndGetCommand;
import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.CompareAndSetCommand;
import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand;
import static com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand.RequestType;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Command codec
 *
 * @author boyan (boyan@alibaba-inc.com)
 * <p>
 * 2018-Apr-25 1:30:30 PM
 */
public class CommandCodec {

    static {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        registry.add(RpcCommand.GetCommand.body);
        registry.add(RpcCommand.SetCommand.body);
        registry.add(RpcCommand.CompareAndSetCommand.body);
        registry.add(RpcCommand.IncrementAndGetCommand.body);
    }

    /**
     * encode the command,returns the byte array.
     *
     * @param requestCommand
     * @return
     */
    public static byte[] encodeCommand(BaseRequestCommand requestCommand) {
        return requestCommand.toByteArray();
    }

    /**
     * Decode the command object from byte array.
     *
     * @param content
     * @param type
     * @return
     */
    public static BaseRequestCommand decodeCommand(byte[] content, RequestType type) {

        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        registry.add(GetCommand.body);
        registry.add(SetCommand.body);
        registry.add(CompareAndSetCommand.body);
        registry.add(IncrementAndGetCommand.body);

        try {
            BaseRequestCommand baseRequestCommand = RpcCommand.BaseRequestCommand.parseFrom(content, registry);
            /*switch (type) {
                case get:
                    return baseRequestCommand.getExtension(GetCommand.body);
                case set:
                    return baseRequestCommand.getExtension(SetCommand.body);
                case compareAndSet:
                    return baseRequestCommand.getExtension(CompareAndSetCommand.body);
                case incrementAndGet:
                    return baseRequestCommand.getExtension(IncrementAndGetCommand.body);
                default:
                    return null;
            }*/
            return baseRequestCommand;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }
}
