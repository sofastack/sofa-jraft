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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntsTest {

    @Test
    public void testCheckedCast() {
        assertEquals(7, Ints.checkedCast(7L));
    }

    @Test
    public void testSaturatedCast() {
        assertEquals(3, Ints.saturatedCast(3L));
        assertEquals(2_147_483_647, Ints.saturatedCast(144_115_190_223_339_520L));
        assertEquals(-2_147_483_648, Ints.saturatedCast(-9_079_256_846_631_436_288L));
    }

    @Test
    public void testFindNextPositivePowerOfTwo() {
        assertEquals(4, Ints.findNextPositivePowerOfTwo(3));
        assertEquals(1, Ints.findNextPositivePowerOfTwo(-99));
        assertEquals(1_073_741_824, Ints.findNextPositivePowerOfTwo(1_107_296_256));
    }

    @Test
    public void testRoundToPowerOfTwo() {
        assertEquals(1_048_576, Ints.roundToPowerOfTwo(524_289));
    }

    @Test
    public void testIsPowerOfTwo() {
        assertTrue(Ints.isPowerOfTwo(2));

        assertFalse(Ints.isPowerOfTwo(3));
    }
}
