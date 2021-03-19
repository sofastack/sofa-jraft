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
package com.alipay.sofa.jraft.rhea.storage.rhea;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.RheaKVServiceFactory;
import com.alipay.sofa.jraft.rhea.client.FutureGroup;
import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVCliService;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;

import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeKey;
import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RheaKVStore unit test, covering all interfaces of {@link RheaKVStore}
 *
 * @author jiachun.fjc
 */
public abstract class AbstractRheaKVStoreTest extends RheaKVTestCluster {

    public abstract StorageType getStorageType();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        System.out.println(">>>>>>>>>>>>>>> Start test method: " + this.testName.getMethodName());
        super.start(getStorageType());
    }

    @After
    public void tearDown() throws Exception {
        System.out.println(">>>>>>>>>>>>>>> Stopping test method: " + this.testName.getMethodName());
        super.shutdown();
        System.out.println(">>>>>>>>>>>>>>> End test method: " + this.testName.getMethodName());
    }

    private void checkRegion(RheaKVStore store, byte[] key, long expectedRegionId) {
        Region region = store.getPlacementDriverClient().findRegionByKey(key, false);
        assertEquals(expectedRegionId, region.getId());
    }

    /**
     * Test method: {@link RheaKVStore#get(byte[])}
     */
    private void getTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        byte[] key = makeKey("a_get_test");
        checkRegion(store, key, 1);
        byte[] value = store.bGet(key);
        assertNull(value);
        value = makeValue("a_get_test_value");
        store.bPut(key, value);
        assertArrayEquals(value, store.bGet(key));

        key = makeKey("h_get_test");
        checkRegion(store, key, 2);
        value = store.bGet(key);
        assertNull(value);
        value = makeValue("h_get_test_value");
        store.bPut(key, value);
        assertArrayEquals(value, store.bGet(key));

        key = makeKey("z_get_test");
        checkRegion(store, key, 2);
        value = store.bGet(key);
        assertNull(value);
        value = makeValue("z_get_test_value");
        store.bPut(key, value);
        assertArrayEquals(value, store.bGet(key));
    }

    @Test
    public void getByLeaderTest() {
        getTest(getRandomLeaderStore());
    }

    @Test
    public void getByFollowerTest() {
        getTest(getRandomFollowerStore());
    }

    @Test
    public void putByLeaderGetByFollower() {
        byte[] key = makeKey("get_test");
        byte[] value = getRandomLeaderStore().bGet(key);
        assertNull(value);
        value = makeValue("get_test_value");
        getRandomLeaderStore().bPut(key, value);
        assertArrayEquals(value, getRandomFollowerStore().bGet(key));
    }

    /**
     * Test method: {@link RheaKVStore#multiGet(List)}
     */
    private void multiGetTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        List<byte[]> keyList = Lists.newArrayList();
        List<byte[]> valueList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("a_multi_test_key_" + i);
            checkRegion(store, key, 1);
            byte[] value = makeValue("a_multi_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            store.bPut(key, value);
        }
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("j_multi_test_key_" + i);
            checkRegion(store, key, 2);
            byte[] value = makeValue("j_multi_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            store.bPut(key, value);
        }
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("t_multi_test_key_" + i);
            checkRegion(store, key, 2);
            byte[] value = makeValue("t_multi_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            store.bPut(key, value);
        }
        keyList.add(makeKey("non_existing_key"));
        Map<ByteArray, byte[]> mapResult = store.bMultiGet(keyList);
        for (int i = 0; i < valueList.size(); i++) {
            byte[] key = keyList.get(i);
            byte[] value = mapResult.get(ByteArray.wrap(key));
            assertArrayEquals(value, valueList.get(i));
        }
        byte[] lastKey = keyList.get(keyList.size() - 1);
        byte[] value = mapResult.get(ByteArray.wrap(lastKey));
        assertNull(value);
    }

    @Test
    public void multiGetByLeaderTest() {
        multiGetTest(getRandomLeaderStore());
    }

    @Test
    public void multiGetByFollowerTest() {
        multiGetTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#containsKey(byte[])}
     */
    private void containsKeyTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        byte[] key = makeKey("a_contains_key_test");
        checkRegion(store, key, 1);
        Boolean isContains = store.bContainsKey(key);
        assertFalse(isContains);
        byte[] value = makeValue("a_contains_key_test_value");
        store.bPut(key, value);
        assertTrue(store.bContainsKey(key));

        key = makeKey("h_contains_key_test");
        checkRegion(store, key, 2);
        isContains = store.bContainsKey(key);
        assertFalse(isContains);
        value = makeValue("h_contains_key_test_value");
        store.bPut(key, value);
        assertTrue(store.bContainsKey(key));

        key = makeKey("z_contains_key_test");
        checkRegion(store, key, 2);
        isContains = store.bContainsKey(key);
        assertFalse(isContains);
        value = makeValue("z_contains_key_test_value");
        store.bPut(key, value);
        assertTrue(store.bContainsKey(key));
    }

    @Test
    public void containsKeyByLeaderTest() {
        containsKeyTest(getRandomLeaderStore());
    }

    @Test
    public void containsKeyByFollowerTest() {
        containsKeyTest(getRandomFollowerStore());
    }

    @Test
    public void scanByLeaderTest() {
        scanTest(getRandomLeaderStore());
    }

    @Test
    public void scanByFollowerTest() {
        scanTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#scan(byte[], byte[])}
     */
    private void scanTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        List<byte[]> keyList = Lists.newArrayList();
        List<byte[]> valueList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("scan_test_key_" + i);
            checkRegion(store, key, 2);
            byte[] value = makeValue("scan_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            store.bPut(key, value);
        }
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("no_scan_test_key_" + i);
            checkRegion(store, key, 2);
            byte[] value = makeValue("no_scan_test_value_" + i);
            store.bPut(key, value);
        }

        // bScan(byte[], byte[])
        {
            List<KVEntry> entries = store.bScan(makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99));
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(i).getValue());
            }
        }

        // bScan(String, String)
        {
            List<KVEntry> entries = store.bScan("scan_test_key_", "scan_test_key_" + 99);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(i).getValue());
            }
        }

        // bScan(byte[], byte[], Boolean)
        {
            List<KVEntry> entries = store.bScan(makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), true);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(i).getValue());
            }
        }

        // bScan(String, String, Boolean)
        {
            List<KVEntry> entries = store.bScan("scan_test_key_", "scan_test_key_" + 99, true);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(i).getValue());
            }
        }

        // bScan(byte[], byte[], Boolean, Boolean)
        {
            List<KVEntry> entries = store.bScan(makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), true, true);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(i).getValue());
            }

            entries = store.bScan(makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), true, false);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(i).getKey());
                assertNull(entries.get(i).getValue());
            }
        }

        // bScan(String, String, Boolean, Boolean)
        {
            List<KVEntry> entries = store.bScan("scan_test_key_", "scan_test_key_" + 99, true, true);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(i).getValue());
            }

            entries = store.bScan("scan_test_key_", "scan_test_key_" + 99, true, false);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(i).getKey());
                assertNull(entries.get(i).getValue());
            }
        }

        {
            List<KVEntry> entries = store.bScan(null, makeKey("no_scan_test_key_" + 99));
            assertEquals(entries.size(), keyList.size());

            entries = store.bScan("no_", null);
            assertEquals(entries.size(), 20);
        }

        {
            List<KVEntry> entries = store.bScan("z", null);
            assertEquals(entries.size(), 0);
        }
    }

    @Test
    public void reverseScanByLeaderTest() {
        reverseScanTest(getRandomLeaderStore());
    }

    @Test
    public void reverseScanByFollowerTest() {
        reverseScanTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#reverseScan(byte[], byte[])}
     */
    private void reverseScanTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        List<byte[]> keyList = Lists.newArrayList();
        List<byte[]> valueList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("scan_test_key_" + i);
            checkRegion(store, key, 2);
            byte[] value = makeValue("scan_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            store.bPut(key, value);
        }
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("no_scan_test_key_" + i);
            checkRegion(store, key, 2);
            byte[] value = makeValue("no_scan_test_value_" + i);
            store.bPut(key, value);
        }

        // bReverseScan(byte[], byte[])
        {
            List<KVEntry> entries = store.bReverseScan(makeKey("scan_test_key_" + 99), makeKey("scan_test_key_"));
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(keyList.size() - 1 - i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(valueList.size() - 1 - i).getValue());
            }
        }

        // bReverseScan(String, String)
        {
            List<KVEntry> entries = store.bReverseScan("scan_test_key_" + 99, "scan_test_key_");
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(keyList.size() - 1 - i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(valueList.size() - 1 - i).getValue());
            }
        }

        // bReverseScan(byte[], byte[], Boolean)
        {
            List<KVEntry> entries = store.bReverseScan(makeKey("scan_test_key_" + 99), makeKey("scan_test_key_"), true);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(keyList.size() - 1 - i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(valueList.size() - 1 - i).getValue());
            }
        }

        // bReverseScan(String, String, Boolean)
        {
            List<KVEntry> entries = store.bReverseScan("scan_test_key_" + 99, "scan_test_key_", true);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(keyList.size() - 1 - i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(valueList.size() - 1 - i).getValue());
            }
        }

        // bReverseScan(byte[], byte[], Boolean, Boolean)
        {
            List<KVEntry> entries = store.bReverseScan(makeKey("scan_test_key_" + 99), makeKey("scan_test_key_"), true,
                true);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(keyList.size() - 1 - i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(valueList.size() - 1 - i).getValue());
            }

            entries = store.bReverseScan(makeKey("scan_test_key_" + 99), makeKey("scan_test_key_"), true, false);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(keyList.size() - 1 - i).getKey());
                assertNull(entries.get(i).getValue());
            }
        }

        // bReverseScan(String, String, Boolean, Boolean)
        {
            List<KVEntry> entries = store.bReverseScan("scan_test_key_" + 99, "scan_test_key_", true, true);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(keyList.size() - 1 - i).getKey());
                assertArrayEquals(valueList.get(i), entries.get(valueList.size() - 1 - i).getValue());
            }

            entries = store.bReverseScan("scan_test_key_" + 99, "scan_test_key_", true, false);
            assertEquals(entries.size(), keyList.size());
            for (int i = 0; i < keyList.size(); i++) {
                assertArrayEquals(keyList.get(i), entries.get(keyList.size() - 1 - i).getKey());
                assertNull(entries.get(i).getValue());
            }
        }

        {
            List<KVEntry> entries = store.bReverseScan(makeKey("no_scan_test_key_" + 99), null);
            assertEquals(entries.size(), keyList.size());

            entries = store.bReverseScan(null, "no_");
            assertEquals(entries.size(), 20);

        }

        {
            List<KVEntry> entries = store.bReverseScan(null, "z");
            assertEquals(entries.size(), 0);
        }
    }

    /**
     * Test method: {@link RheaKVStore#scan(byte[], byte[])}
     */
    private void iteratorTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        List<byte[]> keyList = Lists.newArrayList();
        List<byte[]> valueList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("a_iterator_test_key_" + i);
            checkRegion(store, key, 1);
            byte[] value = makeValue("iterator_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            store.bPut(key, value);
        }
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("h_iterator_test_key_" + i);
            checkRegion(store, key, 2);
            byte[] value = makeValue("iterator_scan_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            store.bPut(key, value);
        }

        // iterator(byte[], byte[], int)
        {
            RheaIterator<KVEntry> iterator = store.iterator(makeKey("a_iterator_test_key_"), makeKey("z"), 2);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList.get(index), kvEntry.getKey());
                assertArrayEquals(valueList.get(index), kvEntry.getValue());
            }
        }

        // iterator(String, String, int)
        {
            RheaIterator<KVEntry> iterator = store.iterator("a_iterator_test_key_", "z", 2);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList.get(index), kvEntry.getKey());
                assertArrayEquals(valueList.get(index), kvEntry.getValue());
            }
        }

        // iterator(byte[], byte[], int, boolean, boolean)
        {
            RheaIterator<KVEntry> iterator = store.iterator(makeKey("a_iterator_test_key_"), makeKey("z"), 2, true,
                false);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList.get(index), kvEntry.getKey());
                assertNull(kvEntry.getValue());
            }
        }

        // iterator(String, String, int, boolean, boolean)
        {
            RheaIterator<KVEntry> iterator = store.iterator("a_iterator_test_key_", "z", 2, true, false);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList.get(index), kvEntry.getKey());
                assertNull(kvEntry.getValue());
            }
        }

        {
            RheaIterator<KVEntry> iterator = store.iterator("a_iterator_test_key_", "z", 2);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList.get(index), kvEntry.getKey());
                assertArrayEquals(valueList.get(index), kvEntry.getValue());
            }
        }

        {
            RheaIterator<KVEntry> iterator = store.iterator("a", null, 5);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList.get(index), kvEntry.getKey());
                assertArrayEquals(valueList.get(index), kvEntry.getValue());
            }
        }
    }

    @Test
    public void iteratorByLeaderTest() {
        iteratorTest(getRandomLeaderStore());
    }

    @Test
    public void iteratorByFollowerTest() {
        iteratorTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#getSequence(byte[], int)}
     */
    private void getSequenceTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        byte[] seqKey = makeKey("seq_test");
        checkRegion(store, seqKey, 2);
        Sequence sequence = store.bGetSequence(seqKey, 199);
        assertEquals(sequence.getStartValue(), 0);
        assertEquals(sequence.getEndValue(), 199);

        // get-only, do not update
        final long latestVal = store.bGetLatestSequence(seqKey);
        assertEquals(latestVal, 199);

        sequence = store.bGetSequence(seqKey, 10);
        assertEquals(sequence.getStartValue(), 199);
        assertEquals(sequence.getEndValue(), 209);

        sequence = store.bGetSequence(seqKey, 10);
        assertEquals(sequence.getStartValue(), 209);
        assertEquals(sequence.getEndValue(), 219);

        assertNull(store.bGet(seqKey));
        store.bResetSequence(seqKey);
        sequence = store.bGetSequence(seqKey, 11);
        assertEquals(sequence.getStartValue(), 0);
        assertEquals(sequence.getEndValue(), 11);
    }

    @Test
    public void getSequenceByLeaderTest() {
        getSequenceTest(getRandomLeaderStore());
    }

    @Test
    public void getSequenceByFollowerTest() {
        getSequenceTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#put(byte[], byte[])}
     */
    private void putTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        byte[] key = makeKey("put_test");
        checkRegion(store, key, 2);
        byte[] value = store.bGet(key);
        assertNull(value);

        value = makeValue("put_test_value");
        store.bPut(key, value);
        byte[] newValue = store.bGet(key);
        assertArrayEquals(value, newValue);
    }

    @Test
    public void putByLeaderTest() {
        putTest(getRandomLeaderStore());
    }

    @Test
    public void putByFollowerTest() {
        putTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#getAndPut(byte[], byte[])}
     */
    private void getAndPutTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        byte[] key = makeKey("put_test");
        checkRegion(store, key, 2);
        byte[] value = store.bGet(key);
        assertNull(value);

        value = makeValue("put_test_value");
        byte[] preVal = store.bGetAndPut(key, value);
        assertNull(preVal);
        byte[] newVal = makeValue("put_test_value_new");
        preVal = store.bGetAndPut(key, newVal);
        assertArrayEquals(value, preVal);
    }

    @Test
    public void getAndPutByLeaderTest() {
        getAndPutTest(getRandomLeaderStore());
    }

    @Test
    public void getAndPutByFollowerTest() {
        getAndPutTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#compareAndPut(byte[], byte[], byte[])}
     */
    private void compareAndPutTest(RheaKVStore store) {
        byte[] key = makeKey("put_test");
        checkRegion(store, key, 2);
        byte[] value = makeValue("put_test_value");
        store.bPut(key, value);

        byte[] update = makeValue("put_test_update");
        assertTrue(store.bCompareAndPut(key, value, update));
        byte[] newValue = store.bGet(key);
        assertArrayEquals(update, newValue);

        assertFalse(store.bCompareAndPut(key, value, update));
    }

    @Test
    public void compareAndPutByLeaderTest() {
        compareAndPutTest(getRandomLeaderStore());
    }

    @Test
    public void compareAndPutByFollowerTest() {
        compareAndPutTest(getRandomFollowerStore());
    }

    /**
     *
     * Test method: {@link RheaKVStore#compareAndPutAll(List)}
     */
    public void compareAndPutAllTest(final RheaKVStore store) {
        final List<CASEntry> entries = new ArrayList<>();
        entries.add(new CASEntry(makeKey("k1"), null, makeValue("v1")));
        entries.add(new CASEntry(makeKey("k2"), null, makeValue("v2")));
        entries.add(new CASEntry(makeKey("k3"), null, makeValue("v3")));

        boolean ret = store.bCompareAndPutAll(entries);
        assertTrue(ret);

        entries.clear();
        entries.add(new CASEntry(makeKey("k1"), makeValue("v1"), makeValue("v11")));
        entries.add(new CASEntry(makeKey("k2"), makeValue("v2"), makeValue("v22")));

        ret = store.bCompareAndPutAll(entries);
        assertTrue(ret);

        entries.clear();
        entries.add(new CASEntry(makeKey("k1"), makeValue("v11"), makeValue("v111")));
        entries.add(new CASEntry(makeKey("k2"), makeValue("v22"), makeValue("v222")));
        entries.add(new CASEntry(makeKey("k3"), makeValue("v33"), makeValue("v333")));

        ret = store.bCompareAndPutAll(entries);
        assertTrue(!ret);

        final Map<ByteArray, byte[]> map = store.bMultiGet(Lists.newArrayList(makeKey("k1"), makeKey("k2"),
            makeKey("k3")));
        assertArrayEquals(makeValue("v11"), map.get(ByteArray.wrap(makeKey("k1"))));
        assertArrayEquals(makeValue("v22"), map.get(ByteArray.wrap(makeKey("k2"))));
        assertArrayEquals(makeValue("v3"), map.get(ByteArray.wrap(makeKey("k3"))));
    }

    @Test
    public void compareAndPutAllByLeaderTest() {
        compareAndPutAllTest(getRandomLeaderStore());
    }

    @Test
    public void compareAndPutAllByFollowerTest() {
        compareAndPutAllTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#merge(String, String)}
     */
    private void mergeTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, t), 3 -> [t, null)
        String key = "key";
        byte[] value = store.bGet(key);
        assertNull(value);

        store.bMerge(key, "aa");
        store.bMerge(key, "bb");
        store.bMerge(key, "cc");
        value = store.bGet(key);
        assertArrayEquals(value, BytesUtil.writeUtf8("aa,bb,cc"));
    }

    @Test
    public void mergeByLeaderTest() {
        mergeTest(getRandomLeaderStore());
    }

    @Test
    public void mergeByFollowerTest() {
        mergeTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#put(List)}
     */
    private void putListTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        List<KVEntry> entries1 = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            byte[] key = makeKey("batch_put_test_key" + i);
            checkRegion(store, key, 1);
            entries1.add(new KVEntry(key, makeValue("batch_put_test_value" + i)));
        }
        store.bPut(entries1);
        List<KVEntry> entries2 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("g_batch_put_test_key" + i);
            checkRegion(store, key, 2);
            entries2.add(new KVEntry(key, makeValue("batch_put_test_value" + i)));
        }
        store.bPut(entries2);
        List<KVEntry> foundList = store.bScan(makeKey("batch_put_test_key"), makeKey("batch_put_test_key" + 99));
        assertEquals(entries1.size(), foundList.size());
        for (int i = 0; i < entries1.size(); i++) {
            assertArrayEquals(entries1.get(i).getKey(), foundList.get(i).getKey());
            assertArrayEquals(entries1.get(i).getValue(), foundList.get(i).getValue());
        }
        foundList = store.bScan(makeKey("g_batch_put_test_key"), makeKey("g_batch_put_test_key" + 99));
        assertEquals(entries2.size(), foundList.size());
        for (int i = 0; i < entries2.size(); i++) {
            assertArrayEquals(entries2.get(i).getKey(), foundList.get(i).getKey());
            assertArrayEquals(entries2.get(i).getValue(), foundList.get(i).getValue());
        }
    }

    @Test
    public void putListByLeaderTest() {
        putListTest(getRandomLeaderStore());
    }

    @Test
    public void putListByFollowerTest1() {
        putListTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#putIfAbsent(byte[], byte[])}
     */
    private void putIfAbsentTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        byte[] key = makeKey("u_put_if_absent_test");
        checkRegion(store, key, 2);
        byte[] value = makeValue("put_if_absent_test_value");
        byte[] newValue = makeValue("put_if_absent_test_value_1");
        byte[] oldValue = store.bPutIfAbsent(key, value);
        assertNull(oldValue);
        oldValue = store.bPutIfAbsent(key, newValue);
        assertArrayEquals(value, oldValue);
    }

    @Test
    public void putIfAbsentByLeaderTest() {
        putIfAbsentTest(getRandomLeaderStore());
    }

    @Test
    public void putIfAbsentByFollowerTest() {
        putIfAbsentTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#delete(byte[])}
     */
    private void deleteTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("del_test" + i);
            checkRegion(store, key, 1);
            byte[] value = makeValue("del_test_value" + i);
            entries.add(new KVEntry(key, value));
        }
        store.bPut(entries);
        store.bDelete(makeKey("del_test5"));
        List<KVEntry> entries2 = store.bScan(makeKey("del_test"), makeKey("del_test" + 99));
        assertEquals(entries.size() - 1, entries2.size());
        byte[] value = store.bGet(makeKey("del_test5"));
        assertNull(value);
    }

    @Test
    public void deleteByLeaderTest() {
        deleteTest(getRandomLeaderStore());
    }

    @Test
    public void deleteByFollowerTest1() {
        deleteTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#deleteRange(byte[], byte[])}
     */
    private void deleteRangeTest(RheaKVStore store) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("del_range_test" + i);
            checkRegion(store, key, 1);
            byte[] value = makeValue("del_range_test_value" + i);
            entries.add(new KVEntry(key, value));
        }
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("t_del_range_test" + i);
            checkRegion(store, key, 2);
            byte[] value = makeValue("del_range_test_value" + i);
            entries.add(new KVEntry(key, value));
        }
        store.bPut(entries);
        // delete [del_range_test5, t_del_range_test8)
        // cover three region: 1 -> [null, g), 2 -> [g, t), 3 -> [t, null)
        store.bDeleteRange(makeKey("del_range_test5"), makeKey("t_del_range_test8"));
        List<KVEntry> entries2 = store.bScan(makeKey("del_range_test"), makeKey("t_del_range_test" + 99));
        assertEquals(entries.size() - 13, entries2.size());
        byte[] value = store.bGet(makeKey("del_range_test5"));
        assertNull(value);
        value = store.bGet(makeKey("del_range_test6"));
        assertNull(value);
        value = store.bGet(makeKey("del_range_test7"));
        assertNull(value);
        value = store.bGet(makeKey("t_del_range_test8"));
        assertNotNull(value);
    }

    @Test
    public void deleteRangeByLeaderTest() {
        deleteRangeTest(getRandomLeaderStore());
    }

    @Test
    public void deleteRangeByFollowerTest() {
        deleteRangeTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#delete(List)}
     */
    private void deleteListTest(RheaKVStore store) {
        List<KVEntry> entries1 = Lists.newArrayList();
        List<byte[]> keys1 = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            byte[] key = makeKey("batch_del_test_key" + i);
            checkRegion(store, key, 1);
            entries1.add(new KVEntry(key, makeValue("batch_del_test_value" + i)));
            keys1.add(key);
        }
        store.bPut(entries1);
        store.bDelete(keys1);
        List<KVEntry> entries2 = Lists.newArrayList();
        List<byte[]> keys2 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("g_batch_del_test_key" + i);
            checkRegion(store, key, 2);
            entries2.add(new KVEntry(key, makeValue("batch_del_test_value" + i)));
            keys2.add(key);
        }
        store.bPut(entries2);
        store.bDelete(keys2);
        List<KVEntry> foundList = store.bScan(makeKey("batch_del_test_key"), makeKey("batch_del_test_key" + 99));
        assertEquals(0, foundList.size());
        for (int i = 0; i < keys1.size(); i++) {
            byte[] value = store.bGet(keys1.get(i));
            assertNull(value);
        }
        foundList = store.bScan(makeKey("g_batch_del_test_key"), makeKey("g_batch_put_test_key" + 99));
        assertEquals(0, foundList.size());
        for (int i = 0; i < keys2.size(); i++) {
            byte[] value = store.bGet(keys2.get(i));
            assertNull(value);
        }
    }

    @Test
    public void deleteListByLeaderTest() {
        deleteListTest(getRandomLeaderStore());
    }

    @Test
    public void deleteListByFollowerTest() {
        deleteListTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#getDistributedLock(byte[], long, TimeUnit)}
     */
    private void distributedLockTest(RheaKVStore store) throws InterruptedException {
        // regions: 1 -> [null, g), 2 -> [g, null)
        byte[] lockKey = makeKey("lock_test");
        checkRegion(store, lockKey, 2);
        final DistributedLock<byte[]> lock = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
        assertNotNull(lock);
        assertTrue(lock.tryLock()); // can lock
        assertTrue(lock.tryLock()); // reentrant lock
        final CountDownLatch latch1 = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            final DistributedLock<byte[]> lock1 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                assertTrue(!lock1.tryLock()); // fail to lock
            } finally {
                latch1.countDown();
            }
        }, "locker1-thread");
        t.setDaemon(true);
        t.start();
        latch1.await();
        final CountDownLatch latch2 = new CountDownLatch(1);
        t = new Thread(() -> {
            final DistributedLock<byte[]> lock2 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                // Waiting for the lock to expire and lock successfully
                assertTrue(lock2.tryLock(3100, TimeUnit.MILLISECONDS));
            } finally {
                lock2.unlock();
                latch2.countDown();
            }
        }, "locker2-thread");
        t.setDaemon(true);
        t.start();
        latch2.await();

        assertTrue(lock.tryLock());
        assertTrue(lock.tryLock());
        assertTrue(lock.tryLock());
        lock.unlock();
        final CountDownLatch latch3 = new CountDownLatch(1);
        t = new Thread(() -> {
            final DistributedLock<byte[]> lock3 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                // The previous lock was not released and the lock failed.
                assertTrue(!lock3.tryLock());
            } finally {
                latch3.countDown();
            }
        }, "locker3-thread");
        t.setDaemon(true);
        t.start();
        latch3.await();

        lock.unlock();
        lock.unlock();
        final CountDownLatch latch4 = new CountDownLatch(1);
        t = new Thread(() -> {
            final DistributedLock<byte[]> lock4 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                assertTrue(lock4.tryLock()); // lock success
            } finally {
                lock4.unlock();
                latch4.countDown();
            }
        }, "locker4-thread");
        t.setDaemon(true);
        t.start();
        latch4.await();

        // lock with lease scheduler
        final ScheduledExecutorService watchdog = Executors.newScheduledThreadPool(1);
        final DistributedLock<byte[]> lockWithScheduler = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS,
                watchdog);
        lockWithScheduler.tryLock();
        Thread.sleep(5000);

        final CountDownLatch latch5 = new CountDownLatch(1);
        t = new Thread(() -> {
            final DistributedLock<byte[]> lock5 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                // Locking failed because lockWithScheduler is renewing
                assertTrue(!lock5.tryLock());
            } finally {
                latch5.countDown();
            }
        }, "locker5-thread");
        t.setDaemon(true);
        t.start();
        latch5.await();

        lockWithScheduler.unlock();

        final CountDownLatch latch6 = new CountDownLatch(1);
        t = new Thread(() -> {
            final DistributedLock<byte[]> lock6 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                assertTrue(lock6.tryLock()); // lock success
            } finally {
                lock6.unlock();
                latch6.countDown();
            }
        }, "locker6-thread");
        t.setDaemon(true);
        t.start();
        latch6.await();
    }

    @Test
    public void distributedLockByLeaderTest() throws InterruptedException {
        distributedLockTest(getRandomLeaderStore());
    }

    @Test
    public void distributedLockByFollowerTest() throws InterruptedException {
        distributedLockTest(getRandomFollowerStore());
    }

    private void batchWriteTest(RheaKVStore store) throws ExecutionException, InterruptedException {
        // regions: 1 -> [null, g), 2 -> [g, t), 3 -> [t, null)
        List<CompletableFuture<Boolean>> futures = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            CompletableFuture<Boolean> f = store.put(makeKey("batch" + i), makeValue("batch"));
            futures.add(f);
        }
        FutureGroup<Boolean> futureGroup = new FutureGroup<>(futures);
        CompletableFuture.allOf(futureGroup.toArray()).get();

        futures.clear();
        for (int i = 0; i < 100; i++) {
            CompletableFuture<Boolean> f = store.delete(makeKey("batch" + i));
            futures.add(f);
        }
        futureGroup = new FutureGroup<>(futures);
        CompletableFuture.allOf(futureGroup.toArray()).get();

        for (int i = 0; i < 100; i++) {
            byte[] value = store.bGet(makeKey("batch" + i));
            assertNull(value);
        }
    }

    @Test
    public void batchWriteByLeaderTest() throws ExecutionException, InterruptedException {
        batchWriteTest(getRandomLeaderStore());
    }

    @Test
    public void batchWriteByFollowerTest() throws ExecutionException, InterruptedException {
        batchWriteTest(getRandomFollowerStore());
    }

    @Test
    public void rangeSplitTest() {
        final RheaKVStore store = getRandomLeaderStore();
        final long regionId = 1;
        for (int i = 0; i < 20; i++) {
            store.bPut("a" + i, BytesUtil.writeUtf8("split"));
        }
        final CliOptions opts = new CliOptions();
        opts.setTimeoutMs(30000);
        final RheaKVCliService cliService = RheaKVServiceFactory.createAndInitRheaKVCliService(opts);
        final long newRegionId = 101;
        final String groupId = JRaftHelper.getJRaftGroupId("rhea_test", regionId);
        final Configuration conf = JRaftUtils.getConfiguration("127.0.0.1:18181,127.0.0.1:18182,127.0.0.1:18183");
        final Status st = cliService.rangeSplit(regionId, newRegionId, groupId, conf);
        System.err.println("Status:" + st);
        assertTrue(st.isOk());
        final RheaKVStore newStore = getLeaderStore(101);
        newStore.bPut("f_first_key", BytesUtil.writeUtf8("split_ok"));
        assertArrayEquals(BytesUtil.writeUtf8("split_ok"), newStore.bGet("f_first_key"));
    }

    @Test
    public void restartAllWithLeaderTest() throws Exception {
        RheaKVStore store = getRandomLeaderStore();
        // regions: 1 -> [null, g), 2 -> [g, null)
        store.bPut("a_get_test", makeValue("a_get_test_value"));
        store.bPut("h_get_test", makeValue("h_get_test_value"));
        store.bPut("z_get_test", makeValue("z_get_test_value"));

        store.bGetSequence("a_seqTest", 10);
        store.bGetSequence("h_seqTest", 11);
        store.bGetSequence("z_seqTest", 12);

        shutdown(false);

        start(getStorageType(), false);

        store = getRandomLeaderStore();

        assertArrayEquals(makeValue("a_get_test_value"), store.bGet("a_get_test"));
        assertArrayEquals(makeValue("h_get_test_value"), store.bGet("h_get_test"));
        assertArrayEquals(makeValue("z_get_test_value"), store.bGet("z_get_test"));

        assertEquals(10, store.bGetSequence("a_seqTest", 1).getStartValue());
        assertEquals(11, store.bGetSequence("h_seqTest", 1).getStartValue());
        assertEquals(12, store.bGetSequence("z_seqTest", 1).getStartValue());
    }

    @Test
    public void restartAllWithFollowerTest() throws Exception {
        RheaKVStore store = getRandomFollowerStore();
        // regions: 1 -> [null, g), 2 -> [g, null)
        store.bPut("a_get_test", makeValue("a_get_test_value"));
        store.bPut("h_get_test", makeValue("h_get_test_value"));
        store.bPut("z_get_test", makeValue("z_get_test_value"));

        store.bGetSequence("a_seqTest", 10);
        store.bGetSequence("h_seqTest", 11);
        store.bGetSequence("z_seqTest", 12);

        shutdown(false);

        start(getStorageType(), false);

        store = getRandomFollowerStore();

        assertArrayEquals(makeValue("a_get_test_value"), store.bGet("a_get_test"));
        assertArrayEquals(makeValue("h_get_test_value"), store.bGet("h_get_test"));
        assertArrayEquals(makeValue("z_get_test_value"), store.bGet("z_get_test"));

        assertEquals(10, store.bGetSequence("a_seqTest", 1).getStartValue());
        assertEquals(11, store.bGetSequence("h_seqTest", 1).getStartValue());
        assertEquals(12, store.bGetSequence("z_seqTest", 1).getStartValue());
    }
}
