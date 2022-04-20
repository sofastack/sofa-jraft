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
package com.alipay.sofa.jraft.rhea.serialization;

import java.nio.ByteBuffer;

import com.alipay.sofa.jraft.util.BufferUtils;
import org.junit.Test;

import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.util.BytesUtil;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author jiachun.fjc
 */
public class SerializerTest {

    @Test
    public void readObjectTest() {
        final Serializer serializer = Serializers.getDefault();
        final KVOperation op = KVOperation.createPut(BytesUtil.writeUtf8("key"), BytesUtil.writeUtf8("value"));
        final byte[] bytes = serializer.writeObject(op);
        final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        BufferUtils.flip(buffer);

        final KVOperation op1 = serializer.readObject(bytes, KVOperation.class);
        final KVOperation op2 = serializer.readObject(buffer, KVOperation.class);

        assertArrayEquals(op1.getKey(), op.getKey());
        assertArrayEquals(op1.getValue(), op.getValue());
        assertEquals(op.getOp(), op.getOp());

        assertArrayEquals(op1.getKey(), op2.getKey());
        assertArrayEquals(op1.getValue(), op2.getValue());
        assertEquals(op.getOp(), op2.getOp());
    }
}
