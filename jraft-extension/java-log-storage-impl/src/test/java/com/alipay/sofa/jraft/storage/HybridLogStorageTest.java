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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.log.RocksDBSegmentLogStorage;
import com.alipay.sofa.jraft.test.TestUtils;

/**
 * @author hzh (642256541@qq.com)
 */
public class HybridLogStorageTest extends BaseStorageTest {
    private HybridLogStorage hybridLogStorage;
    private LogStorage       rocksdbLogStorage;

    @Before
    public void setup() throws Exception {
        super.setup();
        final RaftOptions raftOptions = new RaftOptions();
        this.rocksdbLogStorage = buildRocksdbLogStorage(this.path, raftOptions);
        this.hybridLogStorage = new HybridLogStorage(this.path, this.storeOptions, this.rocksdbLogStorage);
    }

    @After
    public void shutdown() {
        this.hybridLogStorage.shutdown();
    }

    @Test
    public void testTransferLogStorage() {
        final LogStorageOptions opts = newLogStorageOptions();
        Assert.assertTrue(this.rocksdbLogStorage.init(opts));
        // Append 10 logs to rocksdbLogStorage(oldLogStorage)
        for (int i = 1; i <= 10; i++) {
            this.rocksdbLogStorage.appendEntry(TestUtils.mockEntry(i, 1));
        }

        // Init hybridLogStorage
        this.hybridLogStorage = new HybridLogStorage(this.path, this.storeOptions, this.rocksdbLogStorage);
        this.hybridLogStorage.init(opts);
        Assert.assertTrue(this.hybridLogStorage.isOldStorageExist());
        Assert.assertEquals(11, this.hybridLogStorage.getThresholdIndex());

        // Append 10 logs to hybridLogStorage
        for (int i = 11; i <= 20; i++) {
            this.hybridLogStorage.appendEntry(TestUtils.mockEntry(i, 1));
        }
        Assert.assertEquals(20, this.hybridLogStorage.getLastLogIndex());

        // Try truncate, like snapshot
        this.hybridLogStorage.truncatePrefix(11);
        Assert.assertEquals(11, this.hybridLogStorage.getFirstLogIndex());
        Assert.assertFalse(this.hybridLogStorage.isOldStorageExist());

        // Restart
        this.hybridLogStorage.shutdown();
        this.hybridLogStorage.init(opts);
        Assert.assertFalse(this.hybridLogStorage.isOldStorageExist());
        Assert.assertEquals(0, this.hybridLogStorage.getThresholdIndex());
        Assert.assertEquals(11, this.hybridLogStorage.getFirstLogIndex());
        Assert.assertEquals(20, this.hybridLogStorage.getLastLogIndex());
    }

    public LogStorage buildRocksdbLogStorage(final String uri, final RaftOptions raftOptions) {
        return RocksDBSegmentLogStorage.builder(uri, raftOptions) //
            .setPreAllocateSegmentCount(1) //
            .setKeepInMemorySegmentCount(2) //
            .setMaxSegmentFileSize(512 * 1024) //
            .setValueSizeThreshold(0) //
            .build();
    }
}
