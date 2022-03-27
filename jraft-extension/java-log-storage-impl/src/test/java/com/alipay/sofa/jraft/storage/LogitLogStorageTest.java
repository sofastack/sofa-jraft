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
package com.alipay.sofa.jraft.storage;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.storage.db.IndexDB;
import com.alipay.sofa.jraft.storage.file.index.IndexType;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.Pair;

import static org.junit.Assert.assertEquals;

/**
 * @author hzh (642256541@qq.com)
 */
public class LogitLogStorageTest extends BaseLogStorageTest {

    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @After
    public void teardown() throws Exception {
        super.teardown();
    }

    protected LogStorage newLogStorage() {
        return new LogitLogStorage(this.path, this.storeOptions);
    }

    /************************  Test consistency between dbs   ***********************************/

    @Test
    public void testAlignLogWhenLostIndex() {
        final List<LogEntry> entries = TestUtils.mockEntries(20);
        // Set 13 - 16 to be conf entry
        for (int i = 13; i <= 16; i++) {
            final LogEntry entry = entries.get(i);
            entry.setType(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        }
        this.logStorage.appendEntries(entries);
        assertEquals(this.logStorage.getLastLogIndex(), 19);

        // Truncate index db to the index 12, when logStorage reStart, the missing index will be recovered from log db;
        final IndexDB indexDB = ((LogitLogStorage) this.logStorage).getIndexDB();
        indexDB.truncateSuffix(12, 0);
        this.logStorage.shutdown();
        this.logStorage.init(newLogStorageOptions());

        assertEquals(this.logStorage.getLastLogIndex(), 19);
        for (int i = 0; i <= 19; i++) {
            final LogEntry entry = this.logStorage.getEntry(i);
            assertEquals(entry.getId().getIndex(), i);
        }
    }

    @Test
    public void testAlignLogWhenMoreIndex() {
        final List<LogEntry> entries = TestUtils.mockEntries(15);
        this.logStorage.appendEntries(entries);
        // Append more index into indexDB
        final IndexDB indexDB = ((LogitLogStorage) this.logStorage).getIndexDB();
        long maxFlushPosition = 0;
        for (int i = 15; i <= 20; i++) {
            final Pair<Integer, Long> flushPair = indexDB.appendIndexAsync(i, 0, IndexType.IndexSegment);
            maxFlushPosition = Math.max(maxFlushPosition, flushPair.getSecond());
        }
        indexDB.waitForFlush(maxFlushPosition, 100);
        // Recover
        this.logStorage.shutdown();
        this.logStorage.init(newLogStorageOptions());

        // In this case, logitLogStorage will truncate indexdb to the index of 14
        final IndexDB indexDB1 = ((LogitLogStorage) this.logStorage).getIndexDB();
        assertEquals(14, indexDB1.getLastLogIndex());

        for (int i = 0; i <= 14; i++) {
            final LogEntry entry = this.logStorage.getEntry(i);
            assertEquals(entry.getId().getIndex(), i);
        }
    }
}