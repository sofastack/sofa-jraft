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

    static class WrapperInteger implements SegmentList.EstimatedSize {
        int val;

        public WrapperInteger(int val) {
            super();
            this.val = val;
        }

        @Override
        public long estimatedSize() {
            return 4;
        }

    }

    private SegmentList<WrapperInteger> list;

    @Before
    public void setup() {
        this.list = new SegmentList<WrapperInteger>(true);
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
            assertEquals(i, this.list.get(i).val);
        }
        assertEquals(1000, this.list.size());
        assertEquals(4000, this.list.estimatedBytes());
        assertFalse(this.list.isEmpty());
        assertEquals(1000 / SegmentList.SEGMENT_SIZE + 1, this.list.segmentSize());
    }

    private void fillList() {
        int originSize = this.list.size();
        for (int i = 0; i < 1000; i++) {
            this.list.add(new WrapperInteger(i));
            assertEquals(originSize + i + 1, this.list.size());
            assertEquals((originSize + i + 1) * 4, this.list.estimatedBytes());
        }
    }

    @Test
    public void testAddAllGet() {
        List<WrapperInteger> tmpList = new ArrayList<WrapperInteger>();
        for (int i = 0; i < 1000; i++) {
            tmpList.add(new WrapperInteger(i));
        }

        this.list.addAll(tmpList);
        assertFilledList();

        this.list.removeFromFirstWhen(x -> x.val < 100);
        assertEquals(900, this.list.size());
        assertEquals(3600, this.list.estimatedBytes());

        this.list.addAll(tmpList);
        assertEquals(1900, this.list.size());
        assertEquals(1900*4, this.list.estimatedBytes());

        for (int i = 0; i < 1900; i++) {
            if (i < 900) {
                assertEquals(i + 100,  this.list.get(i).val);
            } else {
                assertEquals(i - 900,  this.list.get(i).val);
            }
        }

    }

    @Test
    public void testRemoveFromFirst() {
        fillList();

        int len = SegmentList.SEGMENT_SIZE - 1;
        this.list.removeFromFirst(len);

        assertEquals(1000 - len, this.list.size());
        assertEquals((1000 - len) * 4, this.list.estimatedBytes());

        for (int i = 0; i < 1000 - len; i++) {
            assertEquals(i + len, this.list.get(i).val);
        }

        this.list.removeFromFirst(100);
        assertEquals(1000 - len - 100, this.list.size());

        for (int i = 0; i < 1000 - len - 100; i++) {
            assertEquals(i + len + 100, this.list.get(i).val);
        }

        this.list.removeFromFirst(1000 - len - 100);
        assertTrue(this.list.isEmpty());
        assertEquals(0, this.list.estimatedBytes());
        assertEquals(0, this.list.segmentSize());
        assertNull(this.list.peekFirst());
        assertNull(this.list.peekLast());
    }

    @Test
    public void testRemoveFromFirstWhen() {
        fillList();
        this.list.removeFromFirstWhen(x -> x.val < 200);
        assertEquals(800, this.list.size());
        assertEquals(3200, this.list.estimatedBytes());
        assertEquals(200,   this.list.get(0).val);

        for (int i = 0; i < 800; i++) {
            assertEquals(200 + i,  this.list.get(i).val);
        }

        this.list.removeFromFirstWhen(x -> x.val < 500);
        assertEquals(500, this.list.size());
        assertEquals(2000, this.list.estimatedBytes());
        for (int i = 0; i < 500; i++) {
            assertEquals(500 + i, this.list.get(i).val);
        }

        this.list.removeFromFirstWhen(x -> x.val < 1000);
        assertTrue(this.list.isEmpty());
        assertEquals(0, this.list.estimatedBytes());
        assertEquals(0, this.list.segmentSize());

        fillList();
        assertFilledList();
    }

    @Test
    public void testRemoveFromLastWhen() {
        fillList();

        // remove elements is greater or equal to 150.
        this.list.removeFromLastWhen(x -> x.val >= 150);
        assertEquals(150, this.list.size());
        assertEquals(600, this.list.estimatedBytes());
        assertFalse(this.list.isEmpty());
        for (int i = 0; i < 150; i++) {
            assertEquals(i,  this.list.get(i).val);
        }
        try {
            this.list.get(151);
            fail();
        } catch (IndexOutOfBoundsException e) {

        }
        assertEquals(150 / SegmentList.SEGMENT_SIZE + 1, this.list.segmentSize());

        // remove  elements is greater or equal to 32.
        this.list.removeFromLastWhen(x -> x.val >= 32);
        assertEquals(32, this.list.size());
        assertEquals(128, this.list.estimatedBytes());
        assertFalse(this.list.isEmpty());
        for (int i = 0; i < 32; i++) {
            assertEquals(i,  this.list.get(i).val);
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
        assertEquals(1032*4, this.list.estimatedBytes());
        for (int i = 0; i < 1032; i++) {
            if (i < 32) {
                assertEquals(i,  this.list.get(i).val);
            } else {
                assertEquals(i - 32, this.list.get(i).val);
            }
        }
    }

    @Test
    public void testAddPeek() {
        for (int i = 0; i < 1000; i++) {
            this.list.add(new WrapperInteger(i));
            assertEquals(i, this.list.peekLast().val);
            assertEquals(0, this.list.peekFirst().val);
        }
        assertEquals(4000, this.list.estimatedBytes());
    }

    @Test
    public void simpleBenchmark() {
        int warmupRepeats = 10_0000;
        int repeats = 100_0000;

        double arrayDequeOps = 0;
        double segListOps = 0;
        // test ArrayDequeue
        {
            ArrayDeque<WrapperInteger> deque = new ArrayDeque<>();
            System.gc();
            // wramup
            benchArrayDequeue(warmupRepeats, deque);
            deque.clear();
            System.gc();
            long startNs = System.nanoTime();
            benchArrayDequeue(repeats, deque);
            long costMs = (System.nanoTime() - startNs) / repeats;
            arrayDequeOps = repeats * 3.0 / costMs * 1000;
            System.out.println("ArrayDeque, cost:" + costMs + ", ops: " + arrayDequeOps);
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
            segListOps = repeats * 3.0 / costMs * 1000;
            System.out.println("SegmentList, cost:" + costMs + ", ops: " + segListOps);
            this.list.clear();
        }

        System.out.println("Improvement:" + Math.round((segListOps - arrayDequeOps) / arrayDequeOps * 100) + "%");

    }

    private void benchArrayDequeue(final int repeats, final ArrayDeque<WrapperInteger> deque) {
        int start = 0;
        for (int i = 0; i < repeats; i++) {
            List<WrapperInteger> tmpList = genData(start);
            deque.addAll(tmpList);
            //            for (Integer o : tmpList) {
            //                deque.add(o);
            //            }
            int removePos = start + ThreadLocalRandom.current().nextInt(tmpList.size());

            deque.get(removePos - start);

            int index = 0;
            for (int j = 0; j < deque.size(); j++) {
                if (deque.get(j).val > removePos) {
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

    private List<WrapperInteger> genData(final int start) {
        int n = ThreadLocalRandom.current().nextInt(500) + 10;
        List<WrapperInteger> tmpList = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tmpList.add(new WrapperInteger(i + start));
        }
        return tmpList;
    }

    private void benchSegmentList(final int repeats) {
        int start = 0;

        for (int i = 0; i < repeats; i++) {
            List<WrapperInteger> tmpList = genData(start);
            this.list.addAll(tmpList);
            //            for(Integer o: tmpList) {
            //                list.add(o);
            //            }
            assertEquals(this.list.size()*4, this.list.estimatedBytes());
            int removePos = start + ThreadLocalRandom.current().nextInt(tmpList.size());
            this.list.get(removePos - start);
            this.list.removeFromFirstWhen(x -> x.val <= removePos);
            assertEquals(this.list.size()*4, this.list.estimatedBytes());
            start += tmpList.size();
        }
    }
}
