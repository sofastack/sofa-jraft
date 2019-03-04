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
package com.alipay.sofa.jraft.rhea.util;

import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author jiachun.fjc
 */
@SuppressWarnings("all")
public class VarIntsTest {

    @Test
    public void varInt32Test() {
        for (int i = 0; i < 10000; i++) {
            readWriteVarInt32();
        }
    }

    private void readWriteVarInt32() {
        int value = new Random().nextInt();
        byte[] bytes = VarInts.writeVarInt32(value);
        int newValue = VarInts.readVarInt32(bytes);
        assertEquals(value, newValue);
    }

    @Test
    public void varInt64Test() {
        for (int i = 0; i < 10000; i++) {
            readWriteVarInt64();
        }
    }

    private void readWriteVarInt64() {
        long value = new Random().nextInt();
        byte[] bytes = VarInts.writeVarInt64(value);
        long newValue = VarInts.readVarInt64(bytes);
        assertEquals(value, newValue);
    }
}
