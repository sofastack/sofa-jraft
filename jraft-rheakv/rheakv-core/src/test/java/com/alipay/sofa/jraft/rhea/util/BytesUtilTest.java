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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 *
 * @author jiachun.fjc
 */
public class BytesUtilTest {

    @Test
    public void test() throws UnsupportedEncodingException {
        final String inputString = "The easiest way to write mission-critical real-time applications and microservices with all the benefits of Kafka's server-side cluster technology.";
        final String[] strings = inputString.split("\\W+");
        final List<Pair<String, byte[]>> list1 = Lists.newArrayList();
        final List<Pair<String, byte[]>> list2 = Lists.newArrayList();
        for (final String s : strings) {
            list1.add(Pair.of(s, BytesUtil.writeUtf8(s)));
            list2.add(Pair.of(s, s.getBytes(StandardCharsets.UTF_8)));
        }
        for (int i = 0; i < strings.length; i++) {
            final Pair<String, byte[]> p1 = list1.get(i);
            final Pair<String, byte[]> p2 = list2.get(i);
            assertEquals(p1.getKey(), p2.getKey());
            assertArrayEquals(p1.getValue(), p2.getValue());
            final String fromP1 = BytesUtil.readUtf8(p1.getValue());
            final String fromP2 = BytesUtil.readUtf8(p2.getValue());
            System.out.println("from p1=" + fromP1);
            System.out.println("from p2=" + fromP2);
        }
    }

    @Test
    public void toUtf8BytesTest() {
        for (int i = 0; i < 100000; i++) {
            String in = UUID.randomUUID().toString();
            assertArrayEquals(Utils.getBytes(in), BytesUtil.writeUtf8(in));
        }
    }

    @Test
    public void toUtf8StringTest() {
        for (int i = 0; i < 100000; i++) {
            String str = UUID.randomUUID().toString();
            byte[] in = Utils.getBytes(str);
            assertEquals(new String(in, StandardCharsets.UTF_8), BytesUtil.readUtf8(in));
        }
    }
}
