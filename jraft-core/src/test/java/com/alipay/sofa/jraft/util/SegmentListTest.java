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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.netty.util.internal.ThreadLocalRandom;

public class SegmentListTest {

    private SegmentList<Integer> list;

    @Before
    public void setup() {
        this.list = new SegmentList<>();
    }

    @Test
    public void testAddGet() {
        assertTrue(this.list.isEmpty());
        fillList();
        assertFilledList();
        System.out.println(this.list);
    }

    private void assertFilledList() {
        for (int i = 0; i < 1000; i++) {
            assertEquals(i, (int) this.list.get(i));
        }
        assertEquals(1000, this.list.size());
        assertFalse(this.list.isEmpty());
        assertEquals(1000 / SegmentList.SEGMENT_SIZE + 1, this.list.segmentSize());
    }

    private void fillList() {
        int originSize = this.list.size();
        for (int i = 0; i < 1000; i++) {
            this.list.add(i);
            assertEquals(originSize + i + 1, this.list.size());
        }
    }

    @Test
    public void testAddAllGet() {
        List<Integer> tmpList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            tmpList.add(i);
        }

        this.list.addAll(tmpList);
        assertFilledList();

        this.list.removeFromFirstWhen(x -> x < 100);
        assertEquals(900, this.list.size());

        this.list.addAll(tmpList);
        assertEquals(1900, this.list.size());

        for (int i = 0; i < 1900; i++) {
            if (i < 900) {
                assertEquals(i + 100, (int) this.list.get(i));
            } else {
                assertEquals(i - 900, (int) this.list.get(i));
            }
        }

    }

    @Test
    public void testRemoveFromFirst() {
        fillList();

        this.list.removeFromFirst(31);

        assertEquals(1000 - 31, this.list.size());

        for (int i = 0; i < 1000 - 31; i++) {
            assertEquals(i + 31, (int) this.list.get(i));
        }

        this.list.removeFromFirst(100);
        assertEquals(1000 - 31 - 100, this.list.size());

        for (int i = 0; i < 1000 - 31 - 100; i++) {
            assertEquals(i + 31 + 100, (int) this.list.get(i));
        }

        this.list.removeFromFirst(1000 - 31 - 100);
        assertTrue(this.list.isEmpty());
        assertEquals(0, this.list.segmentSize());
        assertNull(this.list.peekFirst());
        assertNull(this.list.peekLast());
    }

    @Test
    public void testRemoveFromFirstWhen() {
        fillList();
        this.list.removeFromFirstWhen(x -> x < 100);
        assertEquals(900, this.list.size());
        assertEquals(100, (int) this.list.get(0));

        for (int i = 0; i < 900; i++) {
            assertEquals(100 + i, (int) this.list.get(i));
        }

        this.list.removeFromFirstWhen(x -> x < 500);
        assertEquals(500, this.list.size());
        for (int i = 0; i < 500; i++) {
            assertEquals(500 + i, (int) this.list.get(i));
        }

        this.list.removeFromFirstWhen(x -> x < 1000);
        assertTrue(this.list.isEmpty());
        assertEquals(0, this.list.segmentSize());

        fillList();
        assertFilledList();
    }

    @Test
    public void testRemoveFromLastWhen() {
        fillList();

        // remove elements is greater or equal to 50.
        this.list.removeFromLastWhen(x -> x >= 50);
        assertEquals(50, this.list.size());
        assertFalse(this.list.isEmpty());
        for (int i = 0; i < 50; i++) {
            assertEquals(i, (int) this.list.get(i));
        }
        try {
            this.list.get(50);
            fail();
        } catch (IndexOutOfBoundsException e) {

        }
        assertEquals(50 / SegmentList.SEGMENT_SIZE + 1, this.list.segmentSize());

        // remove  elements is greater or equal to 32.
        this.list.removeFromLastWhen(x -> x >= 32);
        assertEquals(32, this.list.size());
        assertFalse(this.list.isEmpty());
        for (int i = 0; i < 32; i++) {
            assertEquals(i, (int) this.list.get(i));
        }
        try {
            this.list.get(32);
            fail();
        } catch (IndexOutOfBoundsException e) {

        }
        assertEquals(1, this.list.segmentSize());

        // Add elements again.
        fillList();
        assertEquals(1032, this.list.size());
        for (int i = 0; i < 1032; i++) {
            if (i < 32) {
                assertEquals(i, (int) this.list.get(i));
            } else {
                assertEquals(i - 32, (int) this.list.get(i));
            }
        }
    }

    @Test
    public void simpleBenchmark() {
        int warmupRepeats = 10_0000;
        int repeats = 100_0000;

        // test ArrayDequeue
        {
            ArrayDeque<Integer> deque = new ArrayDeque<>();
            System.gc();
            // wramup
            benchArrayDequeue(warmupRepeats, deque);
            deque.clear();
            System.gc();
            long startNs = System.nanoTime();
            benchArrayDequeue(repeats, deque);
            long costMs = (System.nanoTime() - startNs) / repeats;
            double ops = repeats * 3.0 / costMs * 1000;
            System.out.println("ArrayDeque, cost:" + costMs + ", ops: " + ops);
        }
        // test SegmentList
        {
            System.gc();
            // warmup
            benchSegmentList(warmupRepeats);

            this.list.clear();
            System.gc();

            long startNs = System.nanoTime();
            benchSegmentList(repeats);
            long costMs = (System.nanoTime() - startNs) / repeats;
            double ops = repeats * 3.0 / costMs * 1000;
            System.out.println("SegmentList, cost:" + costMs + ", ops: " + ops);
            this.list.clear();
        }

    }

    private void benchArrayDequeue(final int repeats, final ArrayDeque<Integer> deque) {
        int start = 0;
        for (int i = 0; i < repeats; i++) {
            List<Integer> tmpList = genData(start);
            deque.addAll(tmpList);
            int removePos = start + ThreadLocalRandom.current().nextInt(tmpList.size());

            deque.get(removePos - start);

            int index = 0;
            for (int j = 0; j < deque.size(); j++) {
                if (deque.get(j) > removePos) {
                    index = j;
                    break;
                }
            }

            if (index > 0) {
                deque.removeRange(0, index);
            }

            start += tmpList.size();
        }
    }

    private List<Integer> genData(final int start) {
        int n = ThreadLocalRandom.current().nextInt(500) + 10;
        List<Integer> tmpList = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tmpList.add(i + start);
        }
        return tmpList;
    }

    private void benchSegmentList(final int repeats) {
        int start = 0;

        for (int i = 0; i < repeats; i++) {
            List<Integer> tmpList = genData(start);
            this.list.addAll(tmpList);
            int removePos = start + ThreadLocalRandom.current().nextInt(tmpList.size());
            this.list.get(removePos - start);
            this.list.removeFromFirstWhen(x -> x <= removePos);
            start += tmpList.size();
        }
    }
}
