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
import java.nio.channels.FileChannel;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.remoting.NamedThreadFactory;
import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage.WriteContext;
import com.alipay.sofa.jraft.storage.log.SegmentFile.SegmentFileOptions;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;

public class SegmentFileTest extends BaseStorageTest {
    private static final int   FILE_SIZE = 64 + SegmentFile.HEADER_SIZE;
    private SegmentFile        segmentFile;
    private ThreadPoolExecutor writeExecutor;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.writeExecutor = ThreadPoolUtil.newThreadPool("test", false, 10, 10, 60, new SynchronousQueue<Runnable>(),
            new NamedThreadFactory("test"));
        String filePath = this.path + File.separator + "SegmentFileTest";
        this.segmentFile = new SegmentFile(FILE_SIZE, filePath, this.writeExecutor);
    }

    @After
    public void tearDown() throws Exception {
        this.segmentFile.shutdown();
        this.writeExecutor.shutdown();
        super.teardown();
    }

    @Test
    public void testSwapInOut() throws Exception {
        testWriteRead();
        this.segmentFile.setReadOnly(true);
        assertFalse(this.segmentFile.isSwappedOut());
        this.segmentFile.swapOut();
        assertTrue(this.segmentFile.isSwappedOut());

        int firstWritePos = SegmentFile.HEADER_SIZE;

        assertEquals(32, this.segmentFile.read(0, firstWritePos).length);
        assertEquals(20, this.segmentFile.read(1, 38 + firstWritePos).length);
        assertFalse(this.segmentFile.isSwappedOut());
    }

    @Test
    public void testInitAndLoad() {
        assertTrue(init());
    }

    private boolean init() {
        SegmentFileOptions opts = SegmentFileOptions.builder() //
            .setRecover(false) //
            .setLastFile(true) //
            .setNewFile(true) //
            .setSync(true) //
            .setPos(0).build();
        return this.segmentFile.init(opts);
    }

    private byte[] genData(final int size) {
        final byte[] bs = new byte[size];
        ThreadLocalRandom.current().nextBytes(bs);
        return bs;
    }

    @Test
    public void testWriteRead() throws Exception {
        init();
        assertFalse(this.segmentFile.isFull());
        int firstWritePos = SegmentFile.HEADER_SIZE;
        assertNull(this.segmentFile.read(0, firstWritePos));
        final byte[] data = genData(32);
        assertFalse(this.segmentFile.reachesFileEndBy(SegmentFile.getWriteBytes(data)));
        WriteContext events = new RocksDBSegmentLogStorage.BarrierWriteContext();
        events.startJob();
        assertEquals(firstWritePos, this.segmentFile.write(0, data, events));
        events.joinAll();
        // Can't read before sync
        assertNull(this.segmentFile.read(0, firstWritePos));
        this.segmentFile.sync(true);
        assertArrayEquals(data, this.segmentFile.read(0, firstWritePos));
        assertTrue(this.segmentFile.reachesFileEndBy(SegmentFile.getWriteBytes(data)));

        final int nextWrotePos = 38 + SegmentFile.HEADER_SIZE;
        assertEquals(nextWrotePos, this.segmentFile.getWrotePos());
        assertEquals(nextWrotePos, this.segmentFile.getCommittedPos());
        assertFalse(this.segmentFile.isFull());
        final byte[] data2 = genData(20);
        assertFalse(this.segmentFile.reachesFileEndBy(SegmentFile.getWriteBytes(data2)));
        events = new RocksDBSegmentLogStorage.BarrierWriteContext();
        events.startJob();
        assertEquals(nextWrotePos, this.segmentFile.write(1, data2, events));
        events.joinAll();
        // Can't read before sync
        assertNull(this.segmentFile.read(1, nextWrotePos));
        this.segmentFile.sync(true);
        assertArrayEquals(data2, this.segmentFile.read(1, nextWrotePos));
        assertEquals(64 + SegmentFile.HEADER_SIZE, this.segmentFile.getWrotePos());
        assertEquals(64 + SegmentFile.HEADER_SIZE, this.segmentFile.getCommittedPos());
        assertTrue(this.segmentFile.isFull());
    }

    @Test
    public void testRecoverFromDirtyMagic() throws Exception {
        testWriteRead();
        int firstWritePos = SegmentFile.HEADER_SIZE;

        SegmentFileOptions opts = SegmentFileOptions.builder() //
            .setRecover(true) //
            .setLastFile(true) //
            .setNewFile(false) //
            .setSync(true) //
            .setPos(0).build();
        {
            // Restart segment file, all data is valid.
            this.segmentFile.shutdown();
            assertTrue(this.segmentFile.init(opts));
            assertEquals(32, this.segmentFile.read(0, firstWritePos).length);
            assertEquals(20, this.segmentFile.read(1, 38 + firstWritePos).length);
        }

        {
            // Corrupted magic bytes at pos=57
            this.segmentFile.clear(39 + firstWritePos, true);
            this.segmentFile.shutdown();
            assertTrue(this.segmentFile.init(opts));
            assertEquals(32, this.segmentFile.read(0, firstWritePos).length);
            assertNull(this.segmentFile.read(1, 38 + firstWritePos));
        }

    }

    @Test
    public void testRecoverFromInvalidData() throws Exception {
        testWriteRead();

        SegmentFileOptions opts = SegmentFileOptions.builder() //
            .setRecover(true) //
            .setLastFile(true) //
            .setNewFile(false) //
            .setSync(true) //
            .setPos(0).build();
        int firstWritePos = SegmentFile.HEADER_SIZE;
        {
            // Restart segment file, all data is valid.
            this.segmentFile.shutdown();
            assertTrue(this.segmentFile.init(opts));
            assertEquals(32, this.segmentFile.read(0, firstWritePos).length);
            assertEquals(20, this.segmentFile.read(1, firstWritePos + 38).length);
        }

        {
            this.segmentFile.shutdown();
            try (FileOutputStream out = new FileOutputStream(new File(this.segmentFile.getPath()), true);
                    FileChannel outChan = out.getChannel()) {
                // Cleared data after pos=62, the second data will be truncated when recovering.
                outChan.truncate(44 + SegmentFile.HEADER_SIZE);
            }
            assertTrue(this.segmentFile.init(opts));
            // First data is still valid
            assertEquals(32, this.segmentFile.read(0, firstWritePos).length);
            // The second data is truncated.
            assertNull(this.segmentFile.read(1, 38 + firstWritePos));
        }

    }
}
