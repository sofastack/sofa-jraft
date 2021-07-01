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

import com.alipay.remoting.NamedThreadFactory;
import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.log.RocksDBSegmentLogStorage;
import com.alipay.sofa.jraft.storage.log.SegmentFile;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage.WriteContext;
import sun.rmi.runtime.Log;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/7/1 16:12
 */
public class LogSegmentTest extends BaseStorageTest {
    private static final int   FILE_SIZE  = 64 + LogSegment.HEADER_SIZE;
    private static final int   INDEX_SIZE = 80;
    private LogSegment         segmentFile;
    private OffsetIndex        offsetIndex;
    private ThreadPoolExecutor writeExecutor;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.writeExecutor = ThreadPoolUtil.newThreadPool("test", false, 10, 10, 60, new SynchronousQueue<Runnable>(),
            new NamedThreadFactory("test"));
        final String filePath = this.path + File.separator + "SegmentFileTest";
        final String indexPath = this.path + File.separator + "OffsetIndexTest";
        this.offsetIndex = new OffsetIndex(indexPath, INDEX_SIZE);
        this.offsetIndex.setBaseOffset(0L);
        this.segmentFile = new LogSegment(FILE_SIZE, filePath, this.writeExecutor, this.offsetIndex);
    }

    @After
    public void tearDown() throws Exception {
        this.offsetIndex.shutdown();
        this.segmentFile.shutdown();
        this.writeExecutor.shutdown();
        super.teardown();
    }

    private boolean init() {
        LogSegment.SegmentFileOptions opts = LogSegment.SegmentFileOptions.builder() //
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
        int firstWritePos = LogSegment.HEADER_SIZE;
        final byte[] data = genData(32);
        assertFalse(this.segmentFile.reachesFileEndBy(LogSegment.getWriteBytes(data)));
        WriteContext events = new RocksDBSegmentLogStorage.BarrierWriteContext();
        events.startJob();
        assertEquals(firstWritePos, this.segmentFile.write(0, data, events));
        events.joinAll();
        // Can't read before sync
        assertNull(this.segmentFile.read(0L));
        this.segmentFile.sync(true);
        assertArrayEquals(this.segmentFile.read(0L), data);

        final int nextWrotePos = 38 + LogSegment.HEADER_SIZE;
        assertEquals(nextWrotePos, this.segmentFile.getWrotePos());
        assertEquals(nextWrotePos, this.segmentFile.getCommittedPos());
        assertFalse(this.segmentFile.isFull());
        final byte[] data2 = genData(20);
        assertFalse(this.segmentFile.reachesFileEndBy(LogSegment.getWriteBytes(data2)));
        events = new RocksDBSegmentLogStorage.BarrierWriteContext();
        events.startJob();
        assertEquals(nextWrotePos, this.segmentFile.write(1, data2, events));
        events.joinAll();
        // Can't read before sync
        assertNull(this.segmentFile.read(1L));
        this.segmentFile.sync(true);
        assertArrayEquals(data2, this.segmentFile.read(1L));

        assertEquals(64 + LogSegment.HEADER_SIZE, this.segmentFile.getWrotePos());
        assertEquals(64 + LogSegment.HEADER_SIZE, this.segmentFile.getCommittedPos());
        assertTrue(this.segmentFile.isFull());
    }

}
