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
package com.alipay.sofa.jraft.storage.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.Utils;

public abstract class BaseLogStorageTest extends BaseStorageTest {
    protected LogStorage         logStorage;
    private ConfigurationManager confManager;
    private LogEntryCodecFactory logEntryCodecFactory;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.confManager = new ConfigurationManager();
        this.logEntryCodecFactory = LogEntryV2CodecFactory.getInstance();
        this.logStorage = newLogStorage();

        final LogStorageOptions opts = newLogStorageOptions();

        this.logStorage.init(opts);
    }

    protected abstract LogStorage newLogStorage();

    protected LogStorageOptions newLogStorageOptions() {
        final LogStorageOptions opts = new LogStorageOptions();
        opts.setConfigurationManager(this.confManager);
        opts.setLogEntryCodecFactory(this.logEntryCodecFactory);
        return opts;
    }

    @Override
    @After
    public void teardown() throws Exception {
        this.logStorage.shutdown();
        super.teardown();
    }

    @Test
    public void testEmptyState() {
        assertEquals(1, this.logStorage.getFirstLogIndex());
        assertEquals(0, this.logStorage.getLastLogIndex());
        assertNull(this.logStorage.getEntry(100));
        assertEquals(0, this.logStorage.getTerm(100));
    }

    @Test
    public void testAddOneEntryState() {
        final LogEntry entry1 = TestUtils.mockEntry(100, 1);
        assertTrue(this.logStorage.appendEntry(entry1));

        assertEquals(100, this.logStorage.getFirstLogIndex());
        assertEquals(100, this.logStorage.getLastLogIndex());
        Assert.assertEquals(entry1, this.logStorage.getEntry(100));
        assertEquals(1, this.logStorage.getTerm(100));

        final LogEntry entry2 = TestUtils.mockEntry(200, 2);
        assertTrue(this.logStorage.appendEntry(entry2));

        assertEquals(100, this.logStorage.getFirstLogIndex());
        assertEquals(200, this.logStorage.getLastLogIndex());
        Assert.assertEquals(entry1, this.logStorage.getEntry(100));
        Assert.assertEquals(entry2, this.logStorage.getEntry(200));
        assertEquals(1, this.logStorage.getTerm(100));
        assertEquals(2, this.logStorage.getTerm(200));
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
        this.logStorage = newLogStorage();
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
    public void testAddManyEntries() {
        final List<LogEntry> entries = TestUtils.mockEntries();

        assertEquals(10, this.logStorage.appendEntries(entries));

        assertEquals(0, this.logStorage.getFirstLogIndex());
        assertEquals(9, this.logStorage.getLastLogIndex());
        for (int i = 0; i < 10; i++) {
            assertEquals(i, this.logStorage.getTerm(i));
            final LogEntry entry = this.logStorage.getEntry(i);
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
        assertEquals(5, this.logStorage.getTerm(5));
    }

    @Test
    public void testTruncatePrefix() {
        final List<LogEntry> entries = TestUtils.mockEntries();

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
    public void testAppendMantyLargeEntries() {
        final long start = Utils.monotonicMs();
        final int totalLogs = 100000;
        final int logSize = 16 * 1024;
        final int batch = 100;

        appendLargeEntries(totalLogs, logSize, batch);

        System.out.println("Inserted " + totalLogs + " large logs, cost " + (Utils.monotonicMs() - start) + " ms.");

        for (int i = 0; i < totalLogs; i++) {
            final LogEntry log = this.logStorage.getEntry(i);
            assertNotNull(log);
            assertEquals(i, log.getId().getIndex());
            assertEquals(i, log.getId().getTerm());
            assertEquals(logSize, log.getData().remaining());
        }

        this.logStorage.shutdown();
        this.logStorage.init(newLogStorageOptions());

        for (int i = 0; i < totalLogs; i++) {
            final LogEntry log = this.logStorage.getEntry(i);
            assertNotNull(log);
            assertEquals(i, log.getId().getIndex());
            assertEquals(i, log.getId().getTerm());
            assertEquals(logSize, log.getData().remaining());
        }
    }

    private void appendLargeEntries(final int totalLogs, final int logSize, final int batch) {
        for (int i = 0; i < totalLogs; i += batch) {
            final List<LogEntry> entries = new ArrayList<>(batch);
            for (int j = i; j < i + batch; j++) {
                entries.add(TestUtils.mockEntry(j, j, logSize));
            }
            assertEquals(batch, this.logStorage.appendEntries(entries));
        }
    }

    @Test
    public void testTruncateSuffix() {
        final List<LogEntry> entries = TestUtils.mockEntries();

        assertEquals(10, this.logStorage.appendEntries(entries));
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
