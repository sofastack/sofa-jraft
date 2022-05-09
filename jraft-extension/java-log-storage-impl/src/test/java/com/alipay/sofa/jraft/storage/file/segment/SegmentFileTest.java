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
package com.alipay.sofa.jraft.storage.file.segment;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.file.FileHeader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * @author hzh (642256541@qq.com)
 */
public class SegmentFileTest extends BaseStorageTest {

    private static final int FILE_SIZE = 64 + FileHeader.HEADER_SIZE;
    private SegmentFile      segmentFile;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        this.init();
    }

    @After
    public void teardown() throws Exception {
        this.segmentFile.shutdown(1000);
        super.teardown();
    }

    private void init() {
        final String filePath = this.path + File.separator + "IndexFileTest";
        this.segmentFile = new SegmentFile(filePath, FILE_SIZE);
    }

    @Test
    public void testAppendDataAndRead() {
        {
            // Write 32 bytes data
            final byte[] data = genData(0, 0, 32);
            int firstWritePos = FileHeader.HEADER_SIZE;
            assertFalse(this.segmentFile.reachesFileEndBy(SegmentFile.getWriteBytes(data)));
            assertEquals(firstWritePos, this.segmentFile.appendData(0, data));
            // Can't read before sync
            this.segmentFile.flush();
            assertArrayEquals(data, this.segmentFile.lookupData(0, firstWritePos));
        }

        {
            // Write 20 bytes data, length = 6 + 14 = 20
            final byte[] data2 = genData(1, 0, 20);
            int nextWrotePos = FileHeader.HEADER_SIZE + 38;
            assertFalse(this.segmentFile.reachesFileEndBy(SegmentFile.getWriteBytes(data2)));
            assertEquals(nextWrotePos, this.segmentFile.appendData(1, data2));
            // Can't read before sync
            this.segmentFile.flush();
            assertArrayEquals(data2, this.segmentFile.lookupData(1, nextWrotePos));
        }
    }

    @Test
    public void testRecoverFromInvalidData() throws IOException {
        testAppendDataAndRead();
        int firstWritePos = FileHeader.HEADER_SIZE;
        {
            // Restart segment file, all data is valid.
            this.segmentFile.shutdown(1000);
            this.init();
            this.segmentFile.recover();
            assertEquals(32, this.segmentFile.lookupData(0, firstWritePos).length);
            assertEquals(20, this.segmentFile.lookupData(1, 38 + firstWritePos).length);
        }

        {
            this.segmentFile.shutdown(1000);
            try (FileOutputStream out = new FileOutputStream(new File(this.segmentFile.getFilePath()), true);
                    FileChannel outChan = out.getChannel()) {
                // Cleared data after pos=64, the second data will be truncated when recovering.
                outChan.truncate(64);
            }
            this.init();
            this.segmentFile.recover();
            // First data is still valid
            assertEquals(32, this.segmentFile.lookupData(0, firstWritePos).length);
            // The second data is truncated.
            assertNull(this.segmentFile.lookupData(1, 38 + firstWritePos));
        }
    }

    @Test
    public void testTruncate() {
        testAppendDataAndRead();
        int truncatePos = FileHeader.HEADER_SIZE + 38;
        this.segmentFile.truncate(1, truncatePos);

        // Recover
        this.segmentFile.shutdown(1000);
        this.init();
        this.segmentFile.recover();

        assertEquals(this.segmentFile.getLastLogIndex(), 0);
    }
}