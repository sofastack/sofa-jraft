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

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;

/**
 * Command codec
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-25 1:30:30 PM
 */
public class CommandCodec {
    /**
     * encode the command,returns the byte array.
     * @param obj
     * @return
     */
    public static byte[] encodeCommand(Object obj) {
        try {
            return SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(obj);
        } catch (final CodecException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Decode the command object from byte array.
     * @param content
     * @param clazz
     * @return
     */
    public static <T> T decodeCommand(byte[] content, Class<T> clazz) {
        try {
            return SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(content, clazz.getName());
        } catch (final CodecException e) {
            throw new IllegalStateException(e);
        }
    }
}
