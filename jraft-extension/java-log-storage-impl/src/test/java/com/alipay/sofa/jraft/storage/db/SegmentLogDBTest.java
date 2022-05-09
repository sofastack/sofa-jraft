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
package com.alipay.sofa.jraft.storage.db;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.util.Pair;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author hzh (642256541@qq.com)
 */
public class SegmentLogDBTest extends BaseStorageTest {
    private SegmentLogDB segmentLogDB;
    private String       segmentStorePath;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        this.segmentStorePath = this.path + File.separator + "segment";
        FileUtils.forceMkdir(new File(this.segmentStorePath));
        this.init();
    }

    public void init() {
        this.segmentLogDB = new SegmentLogDB(this.segmentStorePath);
        this.segmentLogDB.init(this.logStoreFactory);
    }

    @After
    public void teardown() throws Exception {
        this.segmentLogDB.shutdown();
        super.teardown();
    }

    @Test
    public void testAppendLog() throws Exception {
        this.segmentLogDB.startServiceManager();
        // The default file size is 300
        // One entry size = 24 + 6 = 30, so this case will create three log file
        Pair<Integer, Long> posPair = null;
        for (int i = 0; i < 20; i++) {
            final byte[] data = genData(i, 0, 24);
            posPair = this.segmentLogDB.appendLogAsync(i, data);
        }
        this.segmentLogDB.waitForFlush(posPair.getSecond(), 100);
        assertEquals(this.segmentLogDB.getFirstLogIndex(), 0);
        assertEquals(this.segmentLogDB.getLastLogIndex(), 19);
        assertEquals(this.segmentLogDB.getFlushedPosition(), (600 + 26 + 2 * 30));
    }

    @Test
    public void testIterator() throws Exception {
        testAppendLog();
        // Read from the fifth entry, pos = 26 + 30 * 4 = 146
        final AbstractDB.LogEntryIterator iterator = this.segmentLogDB.iterator(LogEntryV2CodecFactory.getInstance()
            .decoder(), 5, 146);
        LogEntry entry;
        int index = 4;
        while ((entry = iterator.next()) != null) {
            assertEquals(index, entry.getId().getIndex());
            index++;
        }
    }

    @Test
    public void testRecover() throws Exception {
        this.segmentLogDB.startServiceManager();
        final byte[] data = genData(1, 0, 150);
        final byte[] data2 = genData(2, 0, 100);
        final byte[] data3 = genData(3, 0, 100);
        {
            // Write first file , one segment file size = 300
            this.segmentLogDB.appendLogAsync(1, data);
            this.segmentLogDB.appendLogAsync(2, data2);
            // Write second file
            final Pair<Integer, Long> posPair = this.segmentLogDB.appendLogAsync(3, data3);
            this.segmentLogDB.waitForFlush(posPair.getSecond(), 100);
        }

        final byte[] log = this.segmentLogDB.lookupLog(3, this.headerSize);
        assertArrayEquals(data3, log);
        {
            this.segmentLogDB.shutdown();
            this.init();
            this.segmentLogDB.recover();
            // Last flush position = one segment file size (300) + header(26) + log3Size(2 + 4 + 100) = 432
            assertEquals(this.segmentLogDB.getFlushedPosition(), 432);
        }
        {
            final byte[] log1 = this.segmentLogDB.lookupLog(1, this.headerSize);
            assertArrayEquals(data, log1);
            final byte[] log3 = this.segmentLogDB.lookupLog(3, this.headerSize);
            assertArrayEquals(data3, log3);
        }
    }

}