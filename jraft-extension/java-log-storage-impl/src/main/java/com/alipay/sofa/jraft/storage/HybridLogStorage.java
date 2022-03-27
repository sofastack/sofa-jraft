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

import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.util.OnlyForTest;

/**
 * HybridLogStorage is used to be compatible with new and old logStorage
 * @author hzh (642256541@qq.com)
 */
public class HybridLogStorage implements LogStorage {
    private static final Logger LOG = LoggerFactory.getLogger(HybridLogStorage.class);

    private volatile LogStorage oldLogStorage;
    private final LogStorage    newLogStorage;
    // The index which separates the oldStorage and newStorage
    private long                thresholdIndex;

    public HybridLogStorage(final String path, final StoreOptions storeOptions, final LogStorage oldStorage) {
        final String newLogStoragePath = Paths.get(path, storeOptions.getStoragePath()).toString();
        this.newLogStorage = new LogitLogStorage(newLogStoragePath, storeOptions);
        this.oldLogStorage = oldStorage;
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        if (this.oldLogStorage != null) {
            if (!this.oldLogStorage.init(opts)) {
                LOG.warn("Init old log storage failed when startup hybridLogStorage");
                return false;
            }
        }
        if (!this.newLogStorage.init(opts)) {
            LOG.warn("Init new log storage failed when startup hybridLogStorage");
            return false;
        }
        this.thresholdIndex = 0;
        if (this.oldLogStorage != null) {
            final long lastLogIndex = this.oldLogStorage.getLastLogIndex();
            if (lastLogIndex == 0) {
                this.oldLogStorage.shutdown();
                this.oldLogStorage = null;
            } else if (lastLogIndex > 0) {
                // Still exists logs in oldLogStorage, need to wait snapshot
                this.thresholdIndex = lastLogIndex + 1;
                LOG.info("Still exists logs in oldLogStorage, lastIndex: {},  need to wait snapshot to truncate logs",
                    lastLogIndex);
            }
        }
        return true;
    }

    @Override
    public void shutdown() {
        if (isOldStorageExist()) {
            this.oldLogStorage.shutdown();
        }
        this.newLogStorage.shutdown();
    }

    @Override
    public long getFirstLogIndex() {
        if (isOldStorageExist()) {
            return this.oldLogStorage.getFirstLogIndex();
        }
        return this.newLogStorage.getFirstLogIndex();
    }

    @Override
    public long getLastLogIndex() {
        if (this.newLogStorage.getLastLogIndex() > 0) {
            return this.newLogStorage.getLastLogIndex();
        }
        if (isOldStorageExist()) {
            return this.oldLogStorage.getLastLogIndex();
        }
        return 0;
    }

    @Override
    public LogEntry getEntry(final long index) {
        if (index >= this.thresholdIndex) {
            return this.newLogStorage.getEntry(index);
        }
        if (isOldStorageExist()) {
            return this.oldLogStorage.getEntry(index);
        }
        return null;
    }

    @Override
    public long getTerm(final long index) {
        if (index >= this.thresholdIndex) {
            return this.newLogStorage.getTerm(index);
        }
        if (isOldStorageExist()) {
            return this.oldLogStorage.getTerm(index);
        }
        return 0;
    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
        return this.newLogStorage.appendEntry(entry);
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
        return this.newLogStorage.appendEntries(entries);
    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        if (!isOldStorageExist()) {
            return this.newLogStorage.truncatePrefix(firstIndexKept);
        }

        if (firstIndexKept < this.thresholdIndex) {
            return this.oldLogStorage.truncatePrefix(firstIndexKept);
        }

        if (isOldStorageExist()) {
            // When firstIndex >= thresholdIndex, we can truncate all logs and shutdown oldStorage
            this.oldLogStorage.truncatePrefix(this.oldLogStorage.getLastLogIndex() + 1);
            this.oldLogStorage.shutdown();
            this.oldLogStorage = null;
            LOG.info("Truncate prefix at logIndex : {}, the thresholdIndex is : {}, shutdown oldLogStorage success!",
                firstIndexKept, this.thresholdIndex);
            this.thresholdIndex = 0;
        }
        return this.newLogStorage.truncatePrefix(firstIndexKept);
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        if (isOldStorageExist()) {
            if (!this.oldLogStorage.truncateSuffix(lastIndexKept)) {
                return false;
            }
        }
        return this.newLogStorage.truncateSuffix(lastIndexKept);
    }

    @Override
    public boolean reset(final long nextLogIndex) {
        if (isOldStorageExist()) {
            if (!this.oldLogStorage.reset(nextLogIndex)) {
                return false;
            }
        }
        return this.newLogStorage.reset(nextLogIndex);
    }

    public boolean isOldStorageExist() {
        return this.oldLogStorage != null;
    }

    @OnlyForTest
    public long getThresholdIndex() {
        return thresholdIndex;
    }
}
