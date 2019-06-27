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
package com.alipay.sofa.jraft.storage.log;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.log.SegmentFile.SegmentFileOptions;

public class SegmentFileTest extends BaseStorageTest {
    private static final int FILE_SIZE = 64;
    private SegmentFile      segmentFile;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.segmentFile = new SegmentFile(0, FILE_SIZE, this.path);
    }

    @After
    public void tearDown() throws Exception {
        this.segmentFile.shutdown();
        super.teardown();
    }

    @Test
    public void testInitAndLoad() {
        assertTrue(init());
    }

    private boolean init() {
        return this.segmentFile.init(new SegmentFileOptions(false, true, 0));
    }

    private byte[] genData(final int size) {
        final byte[] bs = new byte[size];
        ThreadLocalRandom.current().nextBytes(bs);
        return bs;
    }

    @Test
    public void testWriteRead() throws IOException {
        init();
        assertFalse(this.segmentFile.isFull());
        assertNull(this.segmentFile.read(0, 0));
        final byte[] data = genData(32);
        assertFalse(this.segmentFile.reachesFileEndBy(SegmentFile.getWriteBytes(data)));
        assertEquals(0, this.segmentFile.write(0, data));
        // Can't read before sync
        assertNull(this.segmentFile.read(0, 0));
        this.segmentFile.sync();
        assertArrayEquals(data, this.segmentFile.read(0, 0));

        assertTrue(this.segmentFile.reachesFileEndBy(SegmentFile.getWriteBytes(data)));
        assertEquals(38, this.segmentFile.getWrotePos());
        assertEquals(38, this.segmentFile.getCommittedPos());
        assertFalse(this.segmentFile.isFull());
        final byte[] data2 = genData(20);
        assertFalse(this.segmentFile.reachesFileEndBy(SegmentFile.getWriteBytes(data2)));
        assertEquals(38, this.segmentFile.write(1, data2));
        // Can't read before sync
        assertNull(this.segmentFile.read(1, 38));
        this.segmentFile.sync();
        assertArrayEquals(data2, this.segmentFile.read(1, 38));
        assertEquals(64, this.segmentFile.getWrotePos());
        assertEquals(64, this.segmentFile.getCommittedPos());
        assertTrue(this.segmentFile.isFull());
    }

    @Test
    public void testRecoverFromDirtyMagic() throws Exception {
        testWriteRead();

        {
            // Restart segment file, all data is valid.
            this.segmentFile.shutdown();
            assertTrue(this.segmentFile.init(new SegmentFileOptions(true, true, 0)));
            assertEquals(32, this.segmentFile.read(0, 0).length);
            assertEquals(20, this.segmentFile.read(1, 38).length);
        }

        {
            // Corrupted magic bytes at pos=39
            this.segmentFile.clear(39);
            this.segmentFile.shutdown();
            assertTrue(this.segmentFile.init(new SegmentFileOptions(true, true, 0)));
            assertEquals(32, this.segmentFile.read(0, 0).length);
            assertNull(this.segmentFile.read(1, 38));
        }

    }

    @Test
    public void testRecoverFromInvalidData() throws Exception {
        testWriteRead();

        {
            // Restart segment file, all data is valid.
            this.segmentFile.shutdown();
            assertTrue(this.segmentFile.init(new SegmentFileOptions(true, true, 0)));
            assertEquals(32, this.segmentFile.read(0, 0).length);
            assertEquals(20, this.segmentFile.read(1, 38).length);
        }

        {
            // Corrupted magic bytes at pos=39

            this.segmentFile.shutdown();

            try (FileOutputStream out = new FileOutputStream(new File(this.segmentFile.getPath()), true);
                    FileChannel outChan = out.getChannel()) {
                outChan.truncate(44);
            }
            assertTrue(this.segmentFile.init(new SegmentFileOptions(true, true, 0)));
            assertEquals(32, this.segmentFile.read(0, 0).length);
            assertNull(this.segmentFile.read(1, 38));
        }

    }
}
