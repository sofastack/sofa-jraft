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
package com.google.protobuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class ZeroByteStringHelperTest {

    @SuppressWarnings("ConstantConditions")
    @Test
    public void concatenateTest() {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final List<ByteString> byteStrings = new ArrayList<>();
        final List<ByteBuffer> byteBuffers = new ArrayList<>();
        final int segSize = 512;

        int start;
        int end = 0;
        byte[] bytes;

        for (int j = 0; j < 100; j++) {
            start = end;
            end = random.nextInt(start, start + segSize);
            bytes = new byte[end];
            for (int i = start; i < end; i++) {
                bytes[i - start] = (byte) i;
            }
            byteBuffers.add(ByteBuffer.wrap(bytes));
            byteStrings.add(ZeroByteStringHelper.wrap(bytes));
        }
        final ByteString rope = ZeroByteStringHelper.concatenate(byteBuffers);

        int i = 0;
        for (final ByteString bs : byteStrings) {
            for (Byte b : bs) {
                Assert.assertEquals(b.byteValue(), rope.byteAt(i++));
            }
        }
        System.out.println(i);
    }
}
