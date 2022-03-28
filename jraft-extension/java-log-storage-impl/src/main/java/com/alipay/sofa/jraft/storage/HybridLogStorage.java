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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.file.assit.HybridStorageStatusCheckpoint;
import com.alipay.sofa.jraft.util.OnlyForTest;

/**
 * HybridLogStorage is used to be compatible with new and old logStorage
 * @author hzh (642256541@qq.com)
 */
public class HybridLogStorage implements LogStorage {
    private static final Logger                 LOG                    = LoggerFactory
                                                                           .getLogger(HybridLogStorage.class);

    private static final String                 STATUS_CHECKPOINT_PATH = "HybridStatusCheckpoint";

    private final HybridStorageStatusCheckpoint statusCheckpoint;
    private volatile boolean                    isOldStorageExist;
    private final LogStorage                    oldLogStorage;
    private final LogStorage                    newLogStorage;

    // The index which separates the oldStorage and newStorage
    private long                                thresholdIndex;

    public HybridLogStorage(final String path, final StoreOptions storeOptions, final LogStorage oldStorage) {
        final String newLogStoragePath = Paths.get(path, storeOptions.getStoragePath()).toString();
        this.newLogStorage = new LogitLogStorage(newLogStoragePath, storeOptions);
        this.oldLogStorage = oldStorage;
        final String statusCheckpointPath = Paths.get(path, STATUS_CHECKPOINT_PATH).toString();
        this.statusCheckpoint = new HybridStorageStatusCheckpoint(statusCheckpointPath);
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        try {
            this.statusCheckpoint.load();
            this.isOldStorageExist = this.statusCheckpoint.isOldStorageExist;
            if (this.isOldStorageExist) {
                if (this.oldLogStorage != null) {
                    if (!this.oldLogStorage.init(opts)) {
                        LOG.warn("Init old log storage failed when startup hybridLogStorage");
                        return false;
                    }
                    final long lastLogIndex = this.oldLogStorage.getLastLogIndex();
                    if (lastLogIndex == 0) {
                        shutdownOldLogStorage();
                    } else if (lastLogIndex > 0) {
                        // Still exists logs in oldLogStorage, need to wait snapshot
                        this.thresholdIndex = lastLogIndex + 1;
                        this.isOldStorageExist = true;
                        LOG.info(
                            "Still exists logs in oldLogStorage, lastIndex: {},  need to wait snapshot to truncate logs",
                            lastLogIndex);
                    }
                } else {
                    this.isOldStorageExist = false;
                    saveStatusCheckpoint();
                }
            }

            if (!this.newLogStorage.init(opts)) {
                LOG.warn("Init new log storage failed when startup hybridLogStorage");
                return false;
            }
            return true;
        } catch (final IOException e) {
            LOG.error("Error happen when when load hybrid status checkpoint");
            return true;
        }
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

        // When firstIndex >= thresholdIndex, we can reset the old storage the shutdown it.
        if (isOldStorageExist()) {
            this.oldLogStorage.reset(1);
            shutdownOldLogStorage();
            LOG.info("Truncate prefix at logIndex : {}, the thresholdIndex is : {}, shutdown oldLogStorage success!",
                firstIndexKept, this.thresholdIndex);
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
            shutdownOldLogStorage();
        }
        return this.newLogStorage.reset(nextLogIndex);
    }

    private void shutdownOldLogStorage() {
        this.oldLogStorage.shutdown();
        this.isOldStorageExist = false;
        this.thresholdIndex = 0;
        saveStatusCheckpoint();
    }

    private void saveStatusCheckpoint() {
        this.statusCheckpoint.isOldStorageExist = this.isOldStorageExist;
        try {
            // Save status
            this.statusCheckpoint.save();
        } catch (final IOException e) {
            LOG.error("Error happen when save hybrid status checkpoint", e);
        }
    }

    public boolean isOldStorageExist() {
        return this.isOldStorageExist;
    }

    @OnlyForTest
    public long getThresholdIndex() {
        return thresholdIndex;
    }
}
