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
package com.alipay.sofa.jraft.storage.index;

import com.alipay.sofa.jraft.storage.BaseStorageTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test file is based on BASE_OFFSET = 0L
 * @author hzh
 */
public class OffsetIndexTest extends BaseStorageTest {
    private static final int  FILE_SIZE   = 8 * 10;
    private static final Long BASE_OFFSET = 0L;
    private OffsetIndex       offsetIndex;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        final String filePath = this.path + File.separator + "OffsetIndexTest";
        this.offsetIndex = new OffsetIndex(filePath, FILE_SIZE);
        this.offsetIndex.setBaseOffset(0L);
    }

    @After
    public void teardown() throws Exception {
        this.offsetIndex.shutdown();
        super.teardown();
    }

    private final OffsetIndex.IndexEntry appendEntry0 = new OffsetIndex.IndexEntry(0, 1);
    private final OffsetIndex.IndexEntry appendEntry1 = new OffsetIndex.IndexEntry(21, 2);
    private final OffsetIndex.IndexEntry appendEntry2 = new OffsetIndex.IndexEntry(40, 3);

    @Test
    public void testAppend() {
        this.offsetIndex.appendIndex((long) appendEntry0.getOffset(), appendEntry0.getPosition());
        this.offsetIndex.appendIndex((long) appendEntry1.getOffset(), appendEntry1.getPosition());
        this.offsetIndex.appendIndex((long) appendEntry2.getOffset(), appendEntry2.getPosition());
        this.offsetIndex.flush();
    }

    @Test
    public void testLookUp() {
        this.testAppend();

        // Test whether lower bound binary search is true
        OffsetIndex.IndexEntry entry0 = this.offsetIndex.looUp((long) appendEntry0.getOffset());
        assertEquals(appendEntry0.getOffset(), entry0.getOffset());
        OffsetIndex.IndexEntry entry1 = this.offsetIndex.looUp(22L);
        assertEquals(appendEntry1.getOffset(), entry1.getOffset());
        OffsetIndex.IndexEntry entry2 = this.offsetIndex.looUp(45L);
        assertEquals(appendEntry2.getOffset(), entry2.getOffset());
    }

    @Test
    public void testReOpen() throws Exception {
        this.testAppend();

        this.offsetIndex.shutdown();

        // Reopen
        final String filePath = this.path + File.separator + "OffsetIndexTest";
        this.offsetIndex = new OffsetIndex(filePath, FILE_SIZE);
        this.offsetIndex.setBaseOffset(0L);

        // Test look up
        OffsetIndex.IndexEntry entry = this.offsetIndex.looUp((long) appendEntry1.getOffset());
        assertEquals(appendEntry1.getOffset(), entry.getOffset());
    }

    @Test
    public void testTruncate() {
        // Append 10 index entry
        for (int idx = 1; idx <= 10; idx++) {
            this.offsetIndex.appendIndex((long) idx, idx);
        }

        // Check truncate to 9
        this.offsetIndex.truncate(9L);
        assertTrue(this.offsetIndex.getLargestOffset() == 8L);
        this.offsetIndex.appendIndex(9L, 9);

        // Check truncate to 5
        this.offsetIndex.truncate(5L);
        assertTrue(this.offsetIndex.getLargestOffset() == 4L);
    }

}
