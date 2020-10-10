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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.log.RocksDBSegmentLogStorage;
import com.alipay.sofa.jraft.test.TestUtils;

public class RocksDBSegmentLogStorageTest extends BaseLogStorageTest {

    @Override
    protected LogStorage newLogStorage() {
        return new RocksDBSegmentLogStorage(this.path, new RaftOptions(), 0, 1024 * 1024);
    }

    @Test
    public void testTruncateChaos() throws Exception {
        int times = 100;
        int n = 100;

        for (int i = 0; i < times; i++) {
            this.logStorage.shutdown();
            FileUtils.deleteDirectory(new File(this.path));
            this.path = TestUtils.mkTempDir();
            FileUtils.forceMkdir(new File(this.path));
            this.logStorage = new RocksDBSegmentLogStorage(this.path, new RaftOptions(), 32, 256);
            final LogStorageOptions opts = newLogStorageOptions();
            this.logStorage.init(opts);

            for (int j = 0; j < n; j++) {
                this.logStorage.appendEntries(Arrays.asList(TestUtils.mockEntry(j, 1, ThreadLocalRandom.current()
                    .nextInt(180))));
            }

            int index = ThreadLocalRandom.current().nextInt(n);
            boolean truncatePrefix = ThreadLocalRandom.current().nextBoolean();
            if (truncatePrefix) {
                this.logStorage.truncatePrefix(index);

                for (int j = 0; j < n; j++) {
                    if (j < index) {
                        assertNull(this.logStorage.getEntry(j));
                    } else {
                        assertNotNull(this.logStorage.getEntry(j));
                    }
                }
            } else {
                this.logStorage.truncateSuffix(index);

                for (int j = 0; j < n; j++) {
                    if (j <= index) {
                        assertNotNull(this.logStorage.getEntry(j));
                    } else {
                        assertNull(this.logStorage.getEntry(j));
                    }
                }
            }
        }
    }

    @Test
    public void testTruncateSuffixWithDifferentValueSize() throws Exception {
        // shutdown the old one
        this.logStorage.shutdown();
        // Set value threshold to be 32 bytes.
        this.logStorage = new RocksDBSegmentLogStorage(this.path, new RaftOptions(), 32, 64);
        final LogStorageOptions opts = newLogStorageOptions();
        this.logStorage.init(opts);
        int term = 1;

        for (int i = 0; i < 10; i++) {
            this.logStorage.appendEntries(Arrays.asList(TestUtils.mockEntry(i, term, i)));
        }

        this.logStorage.appendEntries(Arrays.asList(TestUtils.mockEntry(10, term, 64)));

        for (int i = 11; i < 20; i++) {
            this.logStorage.appendEntries(Arrays.asList(TestUtils.mockEntry(i, term, i)));
        }

        for (int i = 0; i < 20; i++) {
            assertNotNull(this.logStorage.getEntry(i));
        }

        assertEquals(((RocksDBSegmentLogStorage) this.logStorage).getLastSegmentFileForRead().getWrotePos(), 179);

        this.logStorage.truncateSuffix(15);

        for (int i = 0; i < 20; i++) {
            if (i <= 15) {
                assertNotNull(this.logStorage.getEntry(i));
            } else {
                assertNull(this.logStorage.getEntry(i));
            }
        }

        assertEquals(((RocksDBSegmentLogStorage) this.logStorage).getLastSegmentFileForRead().getWrotePos(), 102);

        this.logStorage.truncateSuffix(13);

        for (int i = 0; i < 20; i++) {
            if (i <= 13) {
                assertNotNull(this.logStorage.getEntry(i));
            } else {
                assertNull(this.logStorage.getEntry(i));
            }
        }

        assertEquals(((RocksDBSegmentLogStorage) this.logStorage).getLastSegmentFileForRead().getWrotePos(), 102);

        this.logStorage.truncateSuffix(5);
        for (int i = 0; i < 20; i++) {
            if (i <= 5) {
                assertNotNull(this.logStorage.getEntry(i));
            } else {
                assertNull(this.logStorage.getEntry(i));
            }
        }

        assertNull(((RocksDBSegmentLogStorage) this.logStorage).getLastSegmentFileForRead());
        this.logStorage.appendEntries(Arrays.asList(TestUtils.mockEntry(20, term, 10)));
        assertNotNull(this.logStorage.getEntry(20));
    }

}
