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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.test.TestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class BaseLogStorageTest extends BaseStorageTest {
    protected LogStorage logStorage;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.logStorage = newLogStorage();

        final LogStorageOptions opts = newLogStorageOptions();

        this.logStorage.init(opts);
    }

    protected abstract LogStorage newLogStorage();

    @Override
    @After
    public void teardown() throws Exception {
        this.logStorage.shutdown();
        super.teardown();
    }

    @Test
    public void testAddOneEntryState() {
        final LogEntry entry1 = TestUtils.mockEntry(100, 1);
        assertTrue(this.logStorage.appendEntry(entry1));
        assertEquals(100, this.logStorage.getFirstLogIndex());
        assertEquals(100, this.logStorage.getLastLogIndex());

        final LogEntry entry2 = TestUtils.mockEntry(101, 2);
        assertTrue(this.logStorage.appendEntry(entry2));
        assertEquals(100, this.logStorage.getFirstLogIndex());
        assertEquals(101, this.logStorage.getLastLogIndex());

        Assert.assertEquals(entry1, this.logStorage.getEntry(100));
        Assert.assertEquals(entry2, this.logStorage.getEntry(101));
    }

    @Test
    public void testAddManyEntries() {
        final List<LogEntry> entries = TestUtils.mockEntries(20);
        assertEquals(20, this.logStorage.appendEntries(entries));
        assertEquals(0, this.logStorage.getFirstLogIndex());
        assertEquals(19, this.logStorage.getLastLogIndex());
        for (int i = 0; i < 20; i++) {
            final LogEntry entry = this.logStorage.getEntry(i);
            assertEquals(entry.getId().getTerm(), i);
            assertNotNull(entry);
            assertEquals(entries.get(i), entry);
        }
    }

    @Test
    public void testReset() {
        testAddManyEntries();
        this.logStorage.reset(5);
        assertEquals(5, this.logStorage.getFirstLogIndex());
        assertEquals(5, this.logStorage.getLastLogIndex());
    }

    @Test
    public void testTruncatePrefix() {
        final List<LogEntry> entries = TestUtils.mockEntries(10);
        assertEquals(10, this.logStorage.appendEntries(entries));
        this.logStorage.truncatePrefix(5);
        assertEquals(5, this.logStorage.getFirstLogIndex());
        assertEquals(9, this.logStorage.getLastLogIndex());
        for (int i = 0; i < 10; i++) {
            if (i < 5) {
                assertNull(this.logStorage.getEntry(i));
            } else {
                Assert.assertEquals(entries.get(i), this.logStorage.getEntry(i));
            }
        }
    }

    @Test
    public void testTruncateSuffix() {
        final List<LogEntry> entries = mockEntries(7, 0);
        final List<LogEntry> confEntries = mockEntries(3, 7);
        for (final LogEntry entry : confEntries) {
            entry.setType(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        }
        {
            // Append 7 segmentLogEntry
            assertEquals(7, this.logStorage.appendEntries(entries));
            // Append 3 confLogEntry
            assertEquals(3, this.logStorage.appendEntries(confEntries));
        }
        {
            // Truncate suffix from 5, that means segmentDB will truncate ( 6 ) and confDB will truncate log ( 7 ~ 9 )
            this.logStorage.truncateSuffix(5);
            assertEquals(0, this.logStorage.getFirstLogIndex());
            assertEquals(5, this.logStorage.getLastLogIndex());
            for (int i = 0; i < 10; i++) {
                if (i <= 5) {
                    Assert.assertEquals(entries.get(i), this.logStorage.getEntry(i));
                } else {
                    assertNull(this.logStorage.getEntry(i));
                }
            }
        }
    }

    public static List<LogEntry> mockEntries(final int n, final int fromIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = fromIndex; i < n + fromIndex; i++) {
            LogEntry entry = TestUtils.mockEntry(i, i);
            if (i > 0) {
                entry.setData(ByteBuffer.wrap(String.valueOf(i).getBytes()));
            }
            entries.add(entry);
        }
        return entries;
    }

    @Test
    public void testLoadWithConfigManager() {
        assertTrue(this.confManager.getLastConfiguration().isEmpty());

        final LogEntry confEntry1 = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        confEntry1.setId(new LogId(99, 1));
        confEntry1.setPeers(JRaftUtils.getConfiguration("localhost:8081,localhost:8082").listPeers());

        final LogEntry confEntry2 = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        confEntry2.setId(new LogId(100, 2));
        confEntry2.setPeers(JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083").listPeers());

        assertTrue(this.logStorage.appendEntry(confEntry1));
        assertEquals(1, this.logStorage.appendEntries(Arrays.asList(confEntry2)));

        // reload log storage.
        this.logStorage.shutdown();
        this.logStorage.init(newLogStorageOptions());

        ConfigurationEntry conf = this.confManager.getLastConfiguration();
        assertNotNull(conf);
        assertFalse(conf.isEmpty());
        assertEquals("localhost:8081,localhost:8082,localhost:8083", conf.getConf().toString());
        conf = this.confManager.get(99);
        assertNotNull(conf);
        assertFalse(conf.isEmpty());
        assertEquals("localhost:8081,localhost:8082", conf.getConf().toString());
    }

    @Test
    public void testRecover() {
        testAddManyEntries();
        this.logStorage.shutdown();
        this.logStorage.init(newLogStorageOptions());
        assertEquals(19, this.logStorage.getLastLogIndex());
    }
}
