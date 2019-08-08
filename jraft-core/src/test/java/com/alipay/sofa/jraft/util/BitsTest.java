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
package com.alipay.sofa.jraft.util;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BitsTest {

    @Test
    public void testGetSet() {
        byte[] bs = new byte[1 + 2 + 4 + 8];

        bs[0] = (byte) 1;
        Bits.putShort(bs, 1, (short) 2);
        Bits.putInt(bs, 3, 3);
        Bits.putLong(bs, 7, 99L);

        assertEquals(1, bs[0]);
        assertEquals((short) 2, Bits.getShort(bs, 1));
        assertEquals(3, Bits.getInt(bs, 3));
        assertEquals(99L, Bits.getLong(bs, 7));
    }

    @Test
    public void testGetDouble() {
        assertEquals(32.0,
            Bits.getDouble(new byte[] {64, 64, 0, 0, 0, 0, 0, 0, 0, 0}, 0), 0.0);
    }

    @Test
    public void testGetFloat() {
        assertEquals(6.0f,
            Bits.getFloat(new byte[] {64, -64, 0, 0, 0, 0}, 0), 0.0f);
    }

    @Test
    public void testPutDouble() {
        byte[] bytes = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        Bits.putDouble(bytes, 2, 4.0);

        assertArrayEquals(new byte[] {0, 0, 64, 16, 0, 0, 0, 0, 0, 0}, bytes);
    }

    @Test
    public void testPutFloat() {
        byte[] bytes = {0, 0, 0, 0, 0, 0, 0, 0};
        Bits.putFloat(bytes, 2, 4.0f);

        assertArrayEquals(new byte[] {0, 0, 64, -128, 0, 0, 0, 0}, bytes);
    }

    @Test
    public void testPutLong() {
        byte[] bytes = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        Bits.putLong(bytes, 2, 2L);

        assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 2}, bytes);
    }
}
