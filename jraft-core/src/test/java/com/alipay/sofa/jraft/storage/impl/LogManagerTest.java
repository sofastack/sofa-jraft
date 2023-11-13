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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Quorum;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.BallotFactory;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.option.LogManagerOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.test.TestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(value = MockitoJUnitRunner.class)
public class LogManagerTest extends BaseStorageTest {
    private static final String  GROUP_ID = "group001";
    private LogManagerImpl       logManager;
    private ConfigurationManager confManager;
    @Mock
    private FSMCaller            fsmCaller;

    private LogStorage           logStorage;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.confManager = new ConfigurationManager();
        final RaftOptions raftOptions = new RaftOptions();
        this.logStorage = newLogStorage(raftOptions);
        this.logManager = new LogManagerImpl();
        final LogManagerOptions opts = new LogManagerOptions();
        opts.setConfigurationManager(this.confManager);
        opts.setLogEntryCodecFactory(LogEntryV2CodecFactory.getInstance());
        opts.setFsmCaller(this.fsmCaller);
        opts.setNodeMetrics(new NodeMetrics(false));
        opts.setLogStorage(this.logStorage);
        opts.setRaftOptions(raftOptions);
        opts.setGroupId(GROUP_ID);
        assertTrue(this.logManager.init(opts));
    }

    protected RocksDBLogStorage newLogStorage(final RaftOptions raftOptions) {
        return new RocksDBLogStorage(this.path, raftOptions);
    }

    @Override
    @After
    public void teardown() throws Exception {
        this.logStorage.shutdown();
        super.teardown();
    }

    @Test
    public void testEmptyState() {
        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(0, this.logManager.getLastLogIndex());
        assertNull(this.logManager.getEntry(1));
        assertEquals(0, this.logManager.getLastLogIndex(true));
        LogId lastLogId = this.logManager.getLastLogId(true);
        assertEquals(0, lastLogId.getIndex());
        lastLogId = this.logManager.getLastLogId(false);
        assertEquals(0, lastLogId.getIndex());
        assertTrue(this.logManager.checkConsistency().isOk());
    }

    @Test
    public void testHasAvailableCapacityToAppendEntries() {
        assertTrue(this.logManager.hasAvailableCapacityToAppendEntries(1));
        assertTrue(this.logManager.hasAvailableCapacityToAppendEntries(10));
        assertFalse(this.logManager.hasAvailableCapacityToAppendEntries(1000000));
    }

    @Test
    public void testAppendOneEntry() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final LogEntry entry = TestUtils.mockEntry(1, 1);
        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);
        this.logManager.appendEntries(entries, new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }
        });
        latch.await();

        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(1, this.logManager.getLastLogIndex());
        Assert.assertEquals(entry, this.logManager.getEntry(1));
        assertEquals(1, this.logManager.getLastLogIndex(true));
        LogId lastLogId = this.logManager.getLastLogId(true);
        assertEquals(1, lastLogId.getIndex());
        lastLogId = this.logManager.getLastLogId(false);
        assertEquals(1, lastLogId.getIndex());
        assertTrue(this.logManager.checkConsistency().isOk());
    }

    @Test
    public void testAppendEntries() throws Exception {
        final List<LogEntry> mockEntries = mockAddEntries();

        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(10, this.logManager.getLastLogIndex());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(mockEntries.get(i), this.logManager.getEntry(i + 1));
        }
        assertEquals(10, this.logManager.getLastLogIndex(true));
        LogId lastLogId = this.logManager.getLastLogId(true);
        assertEquals(10, lastLogId.getIndex());
        lastLogId = this.logManager.getLastLogId(false);
        assertEquals(10, lastLogId.getIndex());
        assertTrue(this.logManager.checkConsistency().isOk());
    }

    @Test
    public void testAppendEntriesBeforeAppliedIndex() throws Exception {
        //Append 0-10
        List<LogEntry> mockEntries = TestUtils.mockEntries(10);
        for (int i = 0; i < 10; i++) {
            mockEntries.get(i).getId().setTerm(1);
        }
        final CountDownLatch latch1 = new CountDownLatch(1);
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch1.countDown();
            }
        });
        latch1.await();

        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(10, this.logManager.getLastLogIndex());
        Thread.sleep(200); // waiting for setDiskId()
        this.logManager.setAppliedId(new LogId(9, 1));

        for (int i = 0; i < 10; i++) {
            assertNull(this.logManager.getEntryFromMemory(i));
        }

        // append 1-10 again, already applied, returns OK.
        final CountDownLatch latch2 = new CountDownLatch(1);
        mockEntries = TestUtils.mockEntries(10);
        mockEntries.remove(0);
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch2.countDown();
            }
        });
        latch2.await();
        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(10, this.logManager.getLastLogIndex());
    }

    @Test
    public void testAppendEntresConflicts() throws Exception {
        //Append 0-10
        List<LogEntry> mockEntries = TestUtils.mockEntries(10);
        for (int i = 0; i < 10; i++) {
            mockEntries.get(i).getId().setTerm(1);
        }
        final CountDownLatch latch1 = new CountDownLatch(1);
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch1.countDown();
            }
        });
        latch1.await();

        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(10, this.logManager.getLastLogIndex());

        //Append 11-20
        final CountDownLatch latch2 = new CountDownLatch(1);
        mockEntries = TestUtils.mockEntries(10);
        for (int i = 0; i < 10; i++) {
            mockEntries.get(i).getId().setIndex(11 + i);
            mockEntries.get(i).getId().setTerm(1);
        }
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch2.countDown();
            }
        });
        latch2.await();

        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(20, this.logManager.getLastLogIndex());

        //Re-adds 11-30, but 15 has different term, it will truncate [14,lastIndex] logs
        mockEntries = TestUtils.mockEntries(20);
        for (int i = 0; i < 20; i++) {
            if (11 + i >= 15) {
                mockEntries.get(i).getId().setTerm(2);
            } else {
                mockEntries.get(i).getId().setTerm(1);
            }
            mockEntries.get(i).getId().setIndex(11 + i);
        }
        final CountDownLatch latch3 = new CountDownLatch(1);
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch3.countDown();
            }
        });
        latch3.await();
        assertEquals(1, this.logManager.getFirstLogIndex());
        assertEquals(30, this.logManager.getLastLogIndex());

        for (int i = 0; i < 30; i++) {
            final LogEntry entry = (this.logManager.getEntry(i + 1));
            assertEquals(i + 1, entry.getId().getIndex());
            if (i + 1 >= 15) {
                assertEquals(2, entry.getId().getTerm());
            } else {
                assertEquals(1, entry.getId().getTerm());
            }
        }
    }

    @Test
    public void testGetConfiguration() throws Exception {
        assertTrue(this.logManager.getConfiguration(1).isEmpty());
        final List<LogEntry> entries = new ArrayList<>(2);
        final LogEntry confEntry1 = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        confEntry1.setId(new LogId(0, 1));
        Configuration conf1 = JRaftUtils.getConfiguration("localhost:8081,localhost:8082");
        BallotFactory.convertConfigToLogEntry(confEntry1, conf1);

        final LogEntry confEntry2 = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        confEntry2.setId(new LogId(0, 2));
        Configuration conf2 = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083");
        confEntry2.setOldPeers(confEntry1.getPeers());
        BallotFactory.convertConfigToLogEntry(confEntry2, conf2);
        BallotFactory.convertOldConfigToLogOuterEntry(confEntry2, conf1);
        entries.add(confEntry1);
        entries.add(confEntry2);

        final CountDownLatch latch = new CountDownLatch(1);
        this.logManager.appendEntries(new ArrayList<>(entries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }
        });
        latch.await();
        ConfigurationEntry entry = this.logManager.getConfiguration(1);
        assertEquals("localhost:8081,localhost:8082,isEnableFlexible:false,quorum:Quorum{w=2, r=2}", entry.getConf()
            .toString());
        assertTrue(entry.getOldConf().isEmpty());

        entry = this.logManager.getConfiguration(2);
        assertEquals("localhost:8081,localhost:8082,localhost:8083,isEnableFlexible:false,quorum:Quorum{w=2, r=2}",
            entry.getConf().toString());
        assertEquals("localhost:8081,localhost:8082,isEnableFlexible:false,quorum:Quorum{w=2, r=2}", entry.getOldConf()
            .toString());
    }

    @Test
    public void testSetAppliedId() throws Exception {
        final List<LogEntry> mockEntries = mockAddEntries();

        for (int i = 0; i < 10; i++) {
            // it's in memory
            Assert.assertEquals(mockEntries.get(i), this.logManager.getEntryFromMemory(i + 1));
        }
        Thread.sleep(200); // waiting for setDiskId()
        this.logManager.setAppliedId(new LogId(10, 10));
        for (int i = 0; i < 10; i++) {
            assertNull(this.logManager.getEntryFromMemory(i + 1));
            Assert.assertEquals(mockEntries.get(i), this.logManager.getEntry(i + 1));
        }
    }

    @Test
    public void testSetAppliedId2() throws Exception {
        final List<LogEntry> mockEntries = mockAddEntries();

        for (int i = 0; i < 10; i++) {
            // it's in memory
            Assert.assertEquals(mockEntries.get(i), this.logManager.getEntryFromMemory(i + 1));
        }
        Thread.sleep(200); // waiting for setDiskId()
        this.logManager.setAppliedId(new LogId(10, 10));
        for (int i = 0; i < 10; i++) {
            assertNull(this.logManager.getEntryFromMemory(i + 1));
            Assert.assertEquals(mockEntries.get(i), this.logManager.getEntry(i + 1));
        }
    }

    private List<LogEntry> mockAddEntries() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final List<LogEntry> mockEntries = TestUtils.mockEntries(10);
        this.logManager.appendEntries(new ArrayList<>(mockEntries), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }
        });
        latch.await();
        return mockEntries;
    }

    @Test
    public void testSetSnapshot() throws Exception {
        final List<LogEntry> entries = mockAddEntries();
        RaftOutter.SnapshotMeta meta = RaftOutter.SnapshotMeta.newBuilder().setLastIncludedIndex(3)
            .setLastIncludedTerm(2).addPeers("localhost:8081").build();
        this.logManager.setSnapshot(meta);
        //Still valid
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(entries.get(i), this.logManager.getEntry(i + 1));
        }
        meta = RaftOutter.SnapshotMeta.newBuilder().setLastIncludedIndex(5).setLastIncludedTerm(4)
            .addPeers("localhost:8081").build();
        this.logManager.setSnapshot(meta);

        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            if (i > 2) {
                Assert.assertEquals(entries.get(i), this.logManager.getEntry(i + 1));
            } else {
                //before index=3 logs were dropped.
                assertNull(this.logManager.getEntry(i + 1));
            }
        }
        assertTrue(this.logManager.checkConsistency().isOk());
    }

    @Test
    public void testWaiter() throws Exception {
        mockAddEntries();
        final Object theArg = new Object();
        final CountDownLatch latch = new CountDownLatch(1);
        final long waitId = this.logManager.wait(10, (arg, errorCode) -> {
            assertSame(arg, theArg);
            assertEquals(0, errorCode);
            latch.countDown();
            return true;
        }, theArg);
        assertEquals(1, waitId);
        mockAddEntries();
        latch.await();
        assertFalse(this.logManager.removeWaiter(waitId));
    }

    @Test
    public void testCheckAndSetConfiguration() throws Exception {
        assertNull(this.logManager.checkAndSetConfiguration(null));
        final ConfigurationEntry entry = new ConfigurationEntry();
        entry.setId(new LogId(0, 1));
        Configuration conf = JRaftUtils.getConfiguration("localhost:8081,localhost:8082");
        Quorum quorum = BallotFactory.buildMajorityQuorum(conf.size());
        conf.setQuorum(quorum);
        conf.setEnableFlexible(false);
        entry.setConf(conf);
        assertSame(entry, this.logManager.checkAndSetConfiguration(entry));

        testGetConfiguration();
        final ConfigurationEntry lastEntry = this.logManager.checkAndSetConfiguration(entry);
        assertNotSame(entry, lastEntry);
        assertEquals("localhost:8081,localhost:8082,localhost:8083,isEnableFlexible:false,quorum:Quorum{w=2, r=2}",
            lastEntry.getConf().toString());
        assertEquals("localhost:8081,localhost:8082,isEnableFlexible:false,quorum:Quorum{w=2, r=2}", lastEntry
            .getOldConf().toString());
    }

}
