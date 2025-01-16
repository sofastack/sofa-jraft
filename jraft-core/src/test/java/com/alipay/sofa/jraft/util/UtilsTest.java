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

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * 
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-10 5:51:20 PM
 */
public class UtilsTest {

    @Test(expected = IllegalArgumentException.class)
    public void tetsVerifyGroupId1() {
        Utils.verifyGroupId("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void tetsVerifyGroupId2() {
        Utils.verifyGroupId(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tetsVerifyGroupId3() {
        Utils.verifyGroupId("1abc");
    }

    @Test(expected = IllegalArgumentException.class)
    public void tetsVerifyGroupId4() {
        Utils.verifyGroupId("*test");
    }

    @Test
    public void tetsVerifyGroupId5() {
        Utils.verifyGroupId("t");
        Utils.verifyGroupId("T");
        Utils.verifyGroupId("Test");
        Utils.verifyGroupId("test");
        Utils.verifyGroupId("test-hello");
        Utils.verifyGroupId("test123");
        Utils.verifyGroupId("t_hello");
    }

    @Test
    public void test_getProcessId() {
        long pid = Utils.getProcessId(-1);
        assertNotEquals(-1, pid);
        System.out.println("test pid:" + pid);
    }

    @Test
    public void testAllocateExpandByteBuffer() {
        ByteBuffer buf = Utils.allocate(128);
        assertEquals(0, buf.position());
        assertEquals(128, buf.capacity());
        assertEquals(128, buf.remaining());

        buf.put("hello".getBytes());
        assertEquals(5, buf.position());

        buf = Utils.expandByteBufferAtLeast(buf, 128);
        assertEquals(5, buf.position());
        assertEquals(1152, buf.capacity());
        assertEquals(1147, buf.remaining());

        buf = Utils.expandByteBufferAtLeast(buf, 2048);
        assertEquals(5, buf.position());
        assertEquals(1152 + 2048, buf.capacity());
        assertEquals(1147 + 2048, buf.remaining());
    }

    @Test
    public void testParsePeerId() {
        String pid = "192.168.1.88:5566";
        String[] result = Utils.parsePeerId(pid);
        String[] expecteds = { "192.168.1.88", "5566" };
        Assert.assertTrue(result.length == 2);
        Assert.assertArrayEquals(expecteds, result);

        pid = "[fe80:0:0:0:6450:aa3c:cd98:ed0f]:8847";
        result = Utils.parsePeerId(pid);
        expecteds = new String[] { "[fe80:0:0:0:6450:aa3c:cd98:ed0f]", "8847" };
        Assert.assertTrue(result.length == 2);
        Assert.assertArrayEquals(expecteds, result);

        pid = "192.168.1.88:5566:9";
        result = Utils.parsePeerId(pid);
        expecteds = new String[] { "192.168.1.88", "5566", "9" };
        Assert.assertTrue(result.length == 3);
        Assert.assertArrayEquals(expecteds, result);

        pid = "[fe80:0:0:0:6450:aa3c:cd98:ed0f]:8847:9";
        result = Utils.parsePeerId(pid);
        expecteds = new String[] { "[fe80:0:0:0:6450:aa3c:cd98:ed0f]", "8847", "9" };
        Assert.assertTrue(result.length == 3);
        Assert.assertArrayEquals(expecteds, result);

        pid = "192.168.1.88:5566:0:6";
        result = Utils.parsePeerId(pid);
        expecteds = new String[] { "192.168.1.88", "5566", "0", "6" };
        Assert.assertTrue(result.length == 4);
        Assert.assertArrayEquals(expecteds, result);

        pid = "[fe80:0:0:0:6450:aa3c:cd98:ed0f]:8847:0:6";
        result = Utils.parsePeerId(pid);
        expecteds = new String[] { "[fe80:0:0:0:6450:aa3c:cd98:ed0f]", "8847", "0", "6" };
        Assert.assertTrue(result.length == 4);
        Assert.assertArrayEquals(expecteds, result);

        boolean ex1 = false;
        try {
            pid = "[192.168.1].88:eee:x:b:j";
            Utils.parsePeerId(pid);
        } catch (Exception e) {
            ex1 = true;
        }
        Assert.assertTrue(ex1);

        boolean ex2 = false;
        try {
            pid = "[dsfsadf]:eee:x:b:j";
            Utils.parsePeerId(pid);
        } catch (Exception e) {
            ex2 = true;
        }
        Assert.assertTrue(ex2);

    }

}
