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
package com.alipay.sofa.jraft.rhea.storage.rocksdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.rocks.support.RocksStatistics;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.KVIterator;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.LocalLock;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.storage.SstColumnFamily;
import com.alipay.sofa.jraft.rhea.storage.SyncKVStore;
import com.alipay.sofa.jraft.rhea.storage.TestClosure;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.ZipUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;

import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeKey;
import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RocksDB unit test, covering all interfaces in {@link RocksRawKVStore}.
 *
 * @author jiachun.fjc
 */
public class RocksKVStoreTest extends BaseKVStoreTest {

    private static final String SNAPSHOT_DIR     = "kv";
    private static final String SNAPSHOT_ARCHIVE = "kv.zip";

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        System.out.println(RocksStatistics.getStatisticsString(this.kvStore));
        super.tearDown();
    }

    /**
     * Test method: {@link RocksRawKVStore#get(byte[], KVStoreClosure)}
     */
    @Test
    public void getTest() {
        final byte[] key = makeKey("get_test");
        byte[] value = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
        assertNull(value);

        value = makeValue("get_test_value");
        this.kvStore.put(key, value, null);
        byte[] newValue = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
        assertArrayEquals(value, newValue);
    }

    /**
     * Test method: {@link RocksRawKVStore#multiGet(List, KVStoreClosure)}
     */
    @SuppressWarnings("unchecked")
    @Test
    public void multiGetTest() {
        final List<byte[]> keyList = Lists.newArrayList();
        final List<byte[]> valueList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("multi_test_key_" + i);
            byte[] value = makeValue("multi_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            this.kvStore.put(key, value, null);
        }
        Map<ByteArray, byte[]> mapResult = new SyncKVStore<Map<ByteArray, byte[]>>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.multiGet(keyList, closure);
            }
        }.apply(this.kvStore);
        for (int i = 0; i < keyList.size(); i++) {
            byte[] key = keyList.get(i);
            assertArrayEquals(mapResult.get(ByteArray.wrap(key)), valueList.get(i));
        }
    }

    /**
     * Test method: {@link RocksRawKVStore#localIterator()}
     */
    @Test
    public void getLocalIteratorTest() {
        final List<byte[]> keyList = Lists.newArrayList();
        final List<byte[]> valueList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("iterator_test_key_" + i);
            byte[] value = makeValue("iterator_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            this.kvStore.put(key, value, null);
        }

        final List<KVEntry> entries = Lists.newArrayList();
        final KVIterator it = this.kvStore.localIterator();
        try {
            it.seekToFirst();
            while (it.isValid()) {
                entries.add(new KVEntry(it.key(), it.value()));
                it.next();
            }
        } finally {
            try {
                it.close();
            } catch (Exception ignored) {
                // ignored
            }
        }

        assertEquals(entries.size(), keyList.size());

        for (int i = 0; i < keyList.size(); i++) {
            assertArrayEquals(keyList.get(i), entries.get(i).getKey());
            assertArrayEquals(valueList.get(i), entries.get(i).getValue());
        }
    }

    /**
     * Test method: {@link RocksRawKVStore#scan(byte[], byte[], KVStoreClosure)}
     */
    @SuppressWarnings("unchecked")
    @Test
    public void scanTest() {
        final List<byte[]> keyList = Lists.newArrayList();
        final List<byte[]> valueList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("scan_test_key_" + i);
            byte[] value = makeValue("scan_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            this.kvStore.put(key, value, null);
        }
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("no_scan_test_key_" + i);
            byte[] value = makeValue("no_scan_test_value_" + i);
            this.kvStore.put(key, value, null);
        }
        List<KVEntry> entries = new SyncKVStore<List<KVEntry>>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.scan(makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), closure);
            }
        }.apply(this.kvStore);
        assertEquals(entries.size(), keyList.size());
        for (int i = 0; i < keyList.size(); i++) {
            assertArrayEquals(keyList.get(i), entries.get(i).getKey());
            assertArrayEquals(valueList.get(i), entries.get(i).getValue());
        }

        entries = new SyncKVStore<List<KVEntry>>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.scan(null, null, closure);
            }
        }.apply(this.kvStore);
        assertEquals(entries.size(), 20);
    }

    /**
     * Test method: {@link RocksRawKVStore#getSequence(byte[], int, KVStoreClosure)}
     */
    @Test
    public void getSequenceTest() throws InterruptedException {
        final byte[] seqKey = makeKey("seq_test");
        Sequence sequence = new SyncKVStore<Sequence>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.getSequence(seqKey, 199, closure);
            }
        }.apply(this.kvStore);
        assertEquals(sequence.getStartValue(), 0);
        assertEquals(sequence.getEndValue(), 199);

        Sequence sequence2 = new SyncKVStore<Sequence>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.getSequence(seqKey, 10, closure);
            }
        }.apply(this.kvStore);
        assertEquals(sequence2.getStartValue(), 199);
        assertEquals(sequence2.getEndValue(), 209);
        this.kvStore.resetSequence(seqKey, null);
        Sequence sequence3 = new SyncKVStore<Sequence>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.getSequence(seqKey, 11, closure);
            }
        }.apply(this.kvStore);
        assertEquals(sequence3.getStartValue(), 0);
        assertEquals(sequence3.getEndValue(), 11);
    }

    /**
     * Test method: {@link RocksRawKVStore#put(byte[], byte[], KVStoreClosure)}
     */
    @Test
    public void putTest() {
        final byte[] key = makeKey("put_test");
        TestClosure closure = new TestClosure();
        this.kvStore.get(key, closure);
        byte[] value = (byte[]) closure.getData();
        assertNull(value);

        value = makeValue("put_test_value");
        this.kvStore.put(key, value, null);
        byte[] newValue = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
        assertArrayEquals(value, newValue);
    }

    /**
     * Test method: {@link RocksRawKVStore#merge(byte[], byte[], KVStoreClosure)}
     */
    @Test
    public void mergeTest() {
        final byte[] key = BytesUtil.writeUtf8("merge_test");
        final byte[] bytes1 = BytesUtil.writeUtf8("a");
        final byte[] bytes2 = BytesUtil.writeUtf8("b");
        final byte[] bytes3 = BytesUtil.writeUtf8("c");
        final byte[] bytes4 = BytesUtil.writeUtf8("d");
        this.kvStore.put(key, bytes1, null);
        this.kvStore.merge(key, bytes2, null);
        this.kvStore.merge(key, bytes3, null);
        this.kvStore.merge(key, bytes4, null);

        final byte[] val = new SyncKVStore<byte[]>() {

            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
        assertArrayEquals(BytesUtil.writeUtf8("a,b,c,d"), val);
    }

    /**
     * Test method: {@link RocksRawKVStore#put(List, KVStoreClosure)}
     */
    @SuppressWarnings("unchecked")
    @Test
    public void putListTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            entries.add(new KVEntry(makeKey("batch_put_test_key" + i), makeValue("batch_put_test_value" + i)));
        }
        this.kvStore.put(entries, null);
        final List<KVEntry> entries2 = new SyncKVStore<List<KVEntry>>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.scan(makeKey("batch_put_test_key"), makeKey("batch_put_test_key" + 99), closure);
            }
        }.apply(this.kvStore);
        assertEquals(entries.size(), entries2.size());
        for (int i = 0; i < entries.size(); i++) {
            assertArrayEquals(entries.get(i).getKey(), entries2.get(i).getKey());
            assertArrayEquals(entries.get(i).getValue(), entries2.get(i).getValue());
        }
    }

    /**
     * Test method: {@link RocksRawKVStore#putIfAbsent(byte[], byte[], KVStoreClosure)}
     */
    @Test
    public void putIfAbsent() {
        byte[] key = makeKey("put_if_absent_test");
        byte[] value = makeValue("put_if_absent_test_value");
        TestClosure closure = new TestClosure();
        this.kvStore.putIfAbsent(key, value, closure);
        assertNull(closure.getData());
        this.kvStore.putIfAbsent(key, value, closure);
        assertArrayEquals(value, (byte[]) closure.getData());
    }

    /**
     * Test method: {@link RocksRawKVStore#tryLockWith(byte[], boolean, DistributedLock.Acquirer, KVStoreClosure)}
     */
    @Test
    public void tryLockWith() throws InterruptedException {
        byte[] lockKey = makeKey("lock_test");
        final DistributedLock<byte[]> lock = new LocalLock(lockKey, 3, TimeUnit.SECONDS, this.kvStore);
        assertNotNull(lock);
        assertTrue(lock.tryLock());
        assertTrue(lock.tryLock());
        assertTrue(lock.tryLock(3001, TimeUnit.MILLISECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            final DistributedLock<byte[]> lock2 = new LocalLock(lockKey, 3, TimeUnit.SECONDS, this.kvStore);
            try {
                assertTrue(!lock2.tryLock());
            } finally {
                latch.countDown();
            }
        }, "lock1-thread").start();
        latch.await();
        lock.unlock();
        assertTrue(lock.tryLock());
        lock.unlock();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void deleteTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            entries.add(new KVEntry(makeKey("del_test" + i), makeValue("del_test_value")));
        }
        this.kvStore.put(entries, null);
        this.kvStore.delete(makeKey("del_test5"), null);
        TestClosure closure = new TestClosure();
        this.kvStore.scan(makeKey("del_test"), makeKey("del_test" + 99), closure);
        List<KVEntry> entries2 = (List<KVEntry>) closure.getData();
        assertEquals(entries.size() - 1, entries2.size());
        closure = new TestClosure();
        this.kvStore.get(makeKey("del_test5"), closure);
        byte[] value = (byte[]) closure.getData();
        assertNull(value);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void deleteRangeTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            entries.add(new KVEntry(makeKey("del_range_test" + i), makeValue("del_range_test" + i)));
        }
        this.kvStore.put(entries, null);
        // delete [del_range_test5, del_range_test8)
        this.kvStore.deleteRange(makeKey("del_range_test5"), makeKey("del_range_test8"), null);
        TestClosure closure = new TestClosure();
        this.kvStore.scan(makeKey("del_range_test"), makeKey("del_range_test" + 99), closure);
        final List<KVEntry> entries2 = (List<KVEntry>) closure.getData();
        assertEquals(entries.size() - 3, entries2.size());
        closure = new TestClosure();
        this.kvStore.get(makeKey("del_range_test5"), closure);
        byte[] value = (byte[]) closure.getData();
        assertNull(value);
        closure = new TestClosure();
        this.kvStore.get(makeKey("del_range_test6"), closure);
        value = (byte[]) closure.getData();
        assertNull(value);
        closure = new TestClosure();
        this.kvStore.get(makeKey("del_range_test7"), closure);
        value = (byte[]) closure.getData();
        assertNull(value);
        closure = new TestClosure();
        this.kvStore.get(makeKey("del_range_test8"), closure);
        value = (byte[]) closure.getData();
        assertNotNull(value);
    }

    @Test
    public void slowSnapshotTest() throws Exception {
        this.dbOptions.setFastSnapshot(false);
        snapshotTest();
    }

    @Test
    public void fastSnapshotTest() throws Exception {
        this.dbOptions.setFastSnapshot(true);
        snapshotTest();
    }

    public void snapshotTest() throws Exception {
        final File backupDir = new File("backup");
        if (backupDir.exists()) {
            FileUtils.deleteDirectory(backupDir);
        }
        FileUtils.forceMkdir(backupDir);
        for (int i = 0; i < 100000; i++) {
            final String v = String.valueOf(i);
            this.kvStore.put(makeKey(v), makeValue(v), null);
        }

        final LocalFileMeta meta = doSnapshotSave(backupDir.getAbsolutePath());

        assertNotNull(get(makeKey("1")));

        this.kvStore.put(makeKey("100001"), makeValue("100001"), null);
        assertNotNull(get(makeKey("100001")));

        this.kvStore.shutdown();
        FileUtils.deleteDirectory(new File(this.tempPath));
        FileUtils.forceMkdir(new File(this.tempPath));
        this.kvStore = new RocksRawKVStore();
        this.kvStore.init(this.dbOptions);

        assertNull(get(makeKey("1")));

        doSnapshotLoad(backupDir.getAbsolutePath(), meta);

        for (int i = 0; i < 100000; i++) {
            final String v = String.valueOf(i);
            assertArrayEquals(makeValue(v), get(makeKey(v)));
        }

        // key[100001] is put after the snapshot, so key[100001] should not exist.
        assertNull(get(makeKey("100001")));

        FileUtils.deleteDirectory(backupDir);
    }

    private byte[] get(final byte[] key) {
        return new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
    }

    private LocalFileMeta doSnapshotSave(final String path) {
        final String snapshotPath = path + File.separator + SNAPSHOT_DIR;
        try {
            final LocalFileMeta meta = this.kvStore.onSnapshotSave(snapshotPath);
            doCompressSnapshot(path);
            return meta;
        } catch (final Throwable t) {
            t.printStackTrace();
        }
        return null;
    }

    public boolean doSnapshotLoad(final String path, final LocalFileMeta meta) {
        try {
            ZipUtil.unzipFile(path + File.separator + SNAPSHOT_ARCHIVE, path);
            this.kvStore.onSnapshotLoad(path + File.separator + SNAPSHOT_DIR, meta);
            return true;
        } catch (final Throwable t) {
            t.printStackTrace();
            return false;
        }
    }

    private void doCompressSnapshot(final String path) {
        try {
            try (final ZipOutputStream out = new ZipOutputStream(new FileOutputStream(path + File.separator
                                                                                      + SNAPSHOT_ARCHIVE))) {
                ZipUtil.compressDirectoryToZipFile(path, SNAPSHOT_DIR, out);
            }
        } catch (final Throwable t) {
            t.printStackTrace();
        }
    }

    @Test
    public void statisticsTest() throws Exception {
        // put
        putTest();

        // close db
        Method closeRocksDB = this.kvStore.getClass().getDeclaredMethod("closeRocksDB");
        closeRocksDB.setAccessible(true);
        closeRocksDB.invoke(this.kvStore);
        Thread.sleep(5000);

        // open db
        Method openRocksDB = this.kvStore.getClass().getDeclaredMethod("openRocksDB", RocksDBOptions.class);
        openRocksDB.setAccessible(true);
        openRocksDB.invoke(this.kvStore, super.dbOptions);

        // get
        getTest();
    }

    @Test
    public void getApproximateKeysInRangeTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10000; i++) {
            entries.add(new KVEntry(makeKey("approximate_test" + i), makeValue("approximate_test_value")));
        }
        this.kvStore.put(entries, null);

        long approximateKeys = this.kvStore.getApproximateKeysInRange(makeKey("approximate_test" + 9999), null);
        assertEquals(1, approximateKeys);
        approximateKeys = this.kvStore.getApproximateKeysInRange(null, makeKey("approximate_test" + 9999));
        assertEquals(10000, approximateKeys);
        approximateKeys = this.kvStore.getApproximateKeysInRange(makeKey("approximate_test" + 9990),
            makeKey("approximate_test" + 9999));
        assertEquals(10, approximateKeys);
    }

    @Test
    public void jumpOverTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10000; i++) {
            entries.add(new KVEntry(makeKey("approximate_test" + i), makeValue("approximate_test_value")));
        }
        this.kvStore.put(entries, null);
        final byte[] endKey = this.kvStore.jumpOver(makeKey("approximate_test0000"), 1000);
        final long approximateKeys = this.kvStore.getApproximateKeysInRange(makeKey("approximate_test0000"), endKey);
        assertEquals(1000, approximateKeys, 100);
    }

    @Test
    public void sstFilesTest() throws IOException {
        for (int i = 0; i < 10000; i++) {
            byte[] bytes = BytesUtil.writeUtf8(String.valueOf(i));
            this.kvStore.put(bytes, bytes, null);
        }
        this.kvStore.getSequence(BytesUtil.writeUtf8("seq"), 100, null);
        final File defaultSstFile = new File("default.sst");
        final File seqSstFile = new File("seq.sst");
        if (defaultSstFile.exists()) {
            FileUtils.forceDelete(defaultSstFile);
        }
        if (seqSstFile.exists()) {
            FileUtils.forceDelete(seqSstFile);
        }
        final EnumMap<SstColumnFamily, File> sstFileTable = new EnumMap<>(SstColumnFamily.class);
        sstFileTable.put(SstColumnFamily.DEFAULT, defaultSstFile);
        sstFileTable.put(SstColumnFamily.SEQUENCE, seqSstFile);
        this.kvStore.createSstFiles(sstFileTable, null, null);
        // remove keys
        for (int i = 0; i < 10000; i++) {
            byte[] bytes = BytesUtil.writeUtf8(String.valueOf(i));
            this.kvStore.delete(bytes, null);
        }
        this.kvStore.resetSequence(BytesUtil.writeUtf8("seq"), null);

        for (int i = 0; i < 10000; i++) {
            byte[] bytes = BytesUtil.writeUtf8(String.valueOf(i));
            TestClosure closure = new TestClosure();
            this.kvStore.get(bytes, closure);
            assertNull(closure.getData());
        }
        this.kvStore.ingestSstFiles(sstFileTable);
        if (defaultSstFile.exists()) {
            FileUtils.forceDelete(defaultSstFile);
        }
        if (seqSstFile.exists()) {
            FileUtils.forceDelete(seqSstFile);
        }
        for (int i = 0; i < 10000; i++) {
            byte[] bytes = BytesUtil.writeUtf8(String.valueOf(i));
            TestClosure closure = new TestClosure();
            this.kvStore.get(bytes, closure);
            assertArrayEquals((byte[]) closure.getData(), bytes);
        }
        TestClosure closure = new TestClosure();
        this.kvStore.getSequence(BytesUtil.writeUtf8("seq"), 100, closure);
        assertEquals(((Sequence) closure.getData()).getStartValue(), 100);
    }
}
