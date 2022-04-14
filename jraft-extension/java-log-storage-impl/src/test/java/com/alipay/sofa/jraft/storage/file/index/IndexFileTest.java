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
package com.alipay.sofa.jraft.storage.file.index;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.file.FileHeader;
import com.alipay.sofa.jraft.storage.file.index.IndexFile.IndexEntry;

import static org.junit.Assert.assertEquals;

/**
 * @author hzh (642256541@qq.com)
 */
public class IndexFileTest extends BaseStorageTest {

    private static final int  FILE_SIZE   = 10 * 10 + FileHeader.HEADER_SIZE;
    private static final long BASE_OFFSET = 0;
    private IndexFile         offsetIndex;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        this.init();
    }

    @After
    public void teardown() throws Exception {
        this.offsetIndex.shutdown(1000);
        super.teardown();
    }

    private void init() {
        final String filePath = this.path + File.separator + "IndexFileTest";
        this.offsetIndex = new IndexFile(filePath, FILE_SIZE);
    }

    private final IndexEntry appendEntry0 = new IndexEntry(0, 1);
    private final IndexEntry appendEntry1 = new IndexEntry(1, 2);
    private final IndexEntry appendEntry2 = new IndexEntry(2, 3);

    @Test
    public void testAppendIndex() {
        this.offsetIndex.appendIndex(appendEntry0.getOffset(), appendEntry0.getPosition(), segmentIndex);
        this.offsetIndex.appendIndex(appendEntry1.getOffset(), appendEntry1.getPosition(), segmentIndex);
        this.offsetIndex.appendIndex(appendEntry2.getOffset(), appendEntry2.getPosition(), segmentIndex);
        this.offsetIndex.flush();
        assertEquals(this.offsetIndex.getLastLogIndex(), appendEntry2.getOffset());
    }

    @Test
    public void testLooUp() {
        testAppendIndex();

        final IndexEntry entry0 = this.offsetIndex.lookupIndex(appendEntry0.getOffset());
        assertEquals(appendEntry0.getOffset(), entry0.getOffset());

        final IndexEntry entry1 = this.offsetIndex.lookupIndex(appendEntry1.getOffset());
        assertEquals(appendEntry1.getOffset(), entry1.getOffset());

        final IndexEntry entry2 = this.offsetIndex.lookupIndex(appendEntry2.getOffset());
        assertEquals(appendEntry2.getOffset(), entry2.getOffset());
    }

    @Test
    public void testTruncate() {
        // Append 10 index entry
        for (int idx = 1; idx <= 10; idx++) {
            this.offsetIndex.appendIndex(idx, idx, segmentIndex);
        }

        // Check truncate to 9
        {
            this.offsetIndex.truncate(9, 0);
            assertEquals(8, this.offsetIndex.getLastLogIndex());
        }

        // Check truncate to 5
        {
            this.offsetIndex.truncate(5, 0);
            // Test recover
            this.offsetIndex.shutdown(1000);
            this.init();
            this.offsetIndex.recover();
            assertEquals(4, this.offsetIndex.getLastLogIndex());
        }
    }

    @Test
    public void testRecoverFromInvalidData() throws Exception {
        testAppendIndex();
        // Reopen
        {
            this.offsetIndex.shutdown(1000);
            this.init();
            this.offsetIndex.recover();
            assertEquals(this.headerSize + 30, this.offsetIndex.getWrotePosition());
            assertEquals(this.offsetIndex.getLastLogIndex(), 2);

            // Test lookup,all data is valid.
            final IndexEntry entry1 = this.offsetIndex.lookupIndex(appendEntry1.getOffset());
            assertEquals(appendEntry1.getOffset(), entry1.getOffset());
        }
        {
            this.offsetIndex.shutdown(1000);
            // Cleared data after pos= 46, the third data will be truncated when recovering.
            try (FileOutputStream out = new FileOutputStream(new File(this.offsetIndex.getFilePath()), true);
                    FileChannel outChan = out.getChannel()) {
                outChan.truncate(this.headerSize + 20);
                out.flush();
            }
            this.init();
            this.offsetIndex.recover();
            assertEquals(this.headerSize + 20, this.offsetIndex.getWrotePosition());
            assertEquals(1, this.offsetIndex.getLastLogIndex());
        }
    }
}