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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.db.AbstractDB;
import com.alipay.sofa.jraft.storage.db.AbstractDB.LogEntryIterator;
import com.alipay.sofa.jraft.storage.db.ConfDB;
import com.alipay.sofa.jraft.storage.db.IndexDB;
import com.alipay.sofa.jraft.storage.db.SegmentLogDB;
import com.alipay.sofa.jraft.storage.factory.LogStoreFactory;
import com.alipay.sofa.jraft.storage.file.FileHeader;
import com.alipay.sofa.jraft.storage.file.assit.FirstLogIndexCheckpoint;
import com.alipay.sofa.jraft.storage.file.index.IndexFile.IndexEntry;
import com.alipay.sofa.jraft.storage.file.index.IndexType;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Pair;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * A logStorage implemented by java
 * @author hzh (642256541@qq.com)
 */
public class LogitLogStorage implements LogStorage {
    private static final Logger           LOG                    = LoggerFactory.getLogger(LogitLogStorage.class);

    private static final String           INDEX_STORE_PATH       = "LogIndex";
    private static final String           SEGMENT_STORE_PATH     = "LogSegment";
    private static final String           CONF_STORE_PATH        = "LogConf";
    private static final String           FIRST_INDEX_CHECKPOINT = "FirstLogIndexCheckpoint";
    private final FirstLogIndexCheckpoint firstLogIndexCheckpoint;
    private final ReadWriteLock           readWriteLock          = new ReentrantReadWriteLock();
    private final Lock                    readLock               = this.readWriteLock.readLock();
    private final Lock                    writeLock              = this.readWriteLock.writeLock();
    private final StoreOptions            storeOptions;
    private final String                  indexStorePath;
    private final String                  segmentStorePath;
    private final String                  confStorePath;
    private ConfigurationManager          configurationManager;
    private LogEntryEncoder               logEntryEncoder;
    private LogEntryDecoder               logEntryDecoder;
    private SegmentLogDB                  segmentLogDB;
    private IndexDB                       indexDB;
    private ConfDB                        confDB;
    private LogStoreFactory               logStoreFactory;

    public LogitLogStorage(final String path, final StoreOptions storeOptions) {
        this.indexStorePath = Paths.get(path, INDEX_STORE_PATH).toString();
        this.segmentStorePath = Paths.get(path, SEGMENT_STORE_PATH).toString();
        this.confStorePath = Paths.get(path, CONF_STORE_PATH).toString();
        this.storeOptions = storeOptions;
        final String checkPointPath = Paths.get(path, FIRST_INDEX_CHECKPOINT).toString();
        this.firstLogIndexCheckpoint = new FirstLogIndexCheckpoint(checkPointPath);
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        Requires.requireNonNull(opts.getConfigurationManager(), "Null conf manager");
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        this.writeLock.lock();
        try {
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            this.configurationManager = opts.getConfigurationManager();

            // Create dbs and recover
            this.logStoreFactory = new LogStoreFactory(this.storeOptions);
            this.indexDB = new IndexDB(this.indexStorePath);
            this.segmentLogDB = new SegmentLogDB(this.segmentStorePath);
            this.confDB = new ConfDB(this.confStorePath);
            if (!(this.indexDB.init(this.logStoreFactory) && this.segmentLogDB.init(this.logStoreFactory) && this.confDB
                .init(this.logStoreFactory))) {
                LOG.warn("Init dbs failed when startup logitLogStorage");
                return false;
            }

            this.firstLogIndexCheckpoint.load();
            return recoverAndLoad();
        } catch (final IOException e) {
            LOG.error("Error on load firstLogIndexCheckPoint", e);
        } finally {
            this.writeLock.unlock();
        }
        return false;
    }

    public boolean recoverAndLoad() {
        this.writeLock.lock();
        try {
            this.indexDB.recover();
            this.segmentLogDB.recover();
            this.confDB.recover();

            // Check consistency
            if (!checkConsistencyAndAlignLog()) {
                LOG.warn("Check the consistency and align log failed");
                return false;
            }

            // Load configuration to conf manager
            loadConfiguration();

            // Set first log index
            if (!this.firstLogIndexCheckpoint.isInit()) {
                saveFirstLogIndex(this.indexDB.getFirstLogIndex());
            }
            LOG.info("Recover dbs and start timingServer success, last recover index:{}",
                this.indexDB.getLastLogIndex());
            return true;
        } catch (final Exception e) {
            LOG.error("Error on recover db", e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Check db's consistency and align the log;
     * @return true if align success;
     */
    private boolean checkConsistencyAndAlignLog() {
        final long lastIndex = this.indexDB.getLastLogIndex();
        final long lastSegmentIndex = this.segmentLogDB.getLastLogIndex();
        final long lastConfIndex = this.confDB.getLastLogIndex();
        if (lastIndex == lastSegmentIndex || lastIndex == lastConfIndex) {
            return true;
        }
        final long maxLogIndex = Math.max(lastSegmentIndex, lastConfIndex);
        if (lastIndex > maxLogIndex) {
            // In this case, just align indexDB to the index of max(lastSegmentIndex, lastConfIndex)
            return this.indexDB.truncateSuffix(maxLogIndex, 0);
        } else {
            // In this case, we should generate indexEntry array sorted by index from segmentDB and confDB, then
            // store indexEntry array to indexDB

            // Step1, lookup last (segment/conf) index in indexDB
            final Pair<IndexEntry, IndexEntry> lastIndexPair = this.indexDB.lookupLastLogIndexAndPosFromTail();
            IndexEntry lastSegmentIndexInfo = lastIndexPair.getFirst();
            IndexEntry lastConfIndexInfo = lastIndexPair.getSecond();

            /**
             * There exists a bad case, for example
             * The index db has index entries 1 ~ 12, but all of the index entries are log index, don't contain conf index
             * The segmentLog db has logs 1 ~ 12 and 16 ~ 19, the conf db has logs 13 ~ 15.
             * So in this case, the lastConfIndexInfo will be null, but we should set it to the first log position
             */
            if (lastSegmentIndexInfo == null) {
                lastSegmentIndexInfo = new IndexEntry(this.segmentLogDB.getFirstLogIndex(), FileHeader.HEADER_SIZE,
                    IndexType.IndexSegment.getType());
            }
            if (lastConfIndexInfo == null) {
                lastConfIndexInfo = new IndexEntry(this.confDB.getFirstLogIndex(), FileHeader.HEADER_SIZE,
                    IndexType.IndexConf.getType());
            }

            // Step2, Using two-way merging algorithm to construct ordered index entry array
            final LogEntryIterator segmentLogIterator = this.segmentLogDB.iterator(this.logEntryDecoder,
                lastSegmentIndexInfo.getLogIndex(), lastSegmentIndexInfo.getPosition());
            final LogEntryIterator confLogIterator = this.confDB.iterator(this.logEntryDecoder,
                lastConfIndexInfo.getLogIndex(), lastConfIndexInfo.getPosition());
            final List<IndexEntry> indexArray = generateOrderedIndexArrayByMergingLogIterator(segmentLogIterator,
                confLogIterator);

            // Step3, store array to indexDB
            long maxFlushPosition = this.indexDB.appendBatchIndexAsync(indexArray);

            // Step4, wait for flushing indexDB
            return this.indexDB.waitForFlush(maxFlushPosition, this.storeOptions.getMaxFlushTimes());
        }
    }

    /**
     * Generate ordered index entry array by using tow-way merging algorithm
     * @param segmentLogIterator segment log iterator
     * @param confLogIterator conf log iterator
     * @return ordered index entry array
     */
    public List<IndexEntry> generateOrderedIndexArrayByMergingLogIterator(final LogEntryIterator segmentLogIterator,
                                                                          final LogEntryIterator confLogIterator) {
        LogEntry segmentEntry = null, confEntry = null;
        int segmentPosition = -1, confPosition = -1;
        final List<IndexEntry> indexEntries = new ArrayList<>();
        while (true) {
            // Pull next entry
            if (segmentEntry == null && segmentLogIterator != null && segmentLogIterator.hasNext()) {
                segmentEntry = segmentLogIterator.next();
                segmentPosition = segmentLogIterator.getReadPosition();
            }
            if (confEntry == null && confLogIterator != null && confLogIterator.hasNext()) {
                confEntry = confLogIterator.next();
                confPosition = confLogIterator.getReadPosition();
            }
            if (segmentEntry == null && confEntry == null) {
                break;
            }
            // Merge
            if (segmentEntry != null && confEntry != null) {
                if (segmentEntry.getId().getIndex() < confEntry.getId().getIndex()) {
                    indexEntries.add(new IndexEntry(segmentEntry.getId().getIndex(), segmentPosition,
                        IndexType.IndexSegment.getType()));
                    segmentEntry = null;
                } else {
                    indexEntries.add(new IndexEntry(confEntry.getId().getIndex(), confPosition, IndexType.IndexConf
                        .getType()));
                    confEntry = null;
                }
            } else {
                indexEntries.add(segmentEntry != null ? new IndexEntry(segmentEntry.getId().getIndex(),
                    segmentPosition, IndexType.IndexSegment.getType()) : new IndexEntry(confEntry.getId().getIndex(),
                    confPosition, IndexType.IndexConf.getType()));
                segmentEntry = confEntry = null;
            }
        }
        return indexEntries;
    }

    private boolean saveFirstLogIndex(final long logIndex) {
        try {
            this.firstLogIndexCheckpoint.setFirstLogIndex(logIndex);
            return this.firstLogIndexCheckpoint.save();
        } catch (final IOException e) {
            LOG.error("Error when save first log index", e);
            return false;
        }
    }

    /**
     * Load configuration logEntries in confDB to configurationManager
     */
    public void loadConfiguration() {
        final LogEntryIterator confIterator = this.confDB.iterator(this.logEntryDecoder);
        LogEntry entry;
        while ((entry = confIterator.next()) != null) {
            if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                final ConfigurationEntry confEntry = new ConfigurationEntry();
                confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                confEntry.setConf(new Configuration(entry.getPeers(), entry.getLearners()));
                if (entry.getOldPeers() != null) {
                    confEntry.setOldConf(new Configuration(entry.getOldPeers(), entry.getOldLearners()));
                }
                if (this.configurationManager != null) {
                    this.configurationManager.add(confEntry);
                }
            }
        }
    }

    /****************************  Implementation   ********************************/

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            if (this.firstLogIndexCheckpoint.firstLogIndex >= 0) {
                return this.firstLogIndexCheckpoint.firstLogIndex;
            } else if (this.indexDB.getFirstLogIndex() >= 0) {
                return this.indexDB.getFirstLogIndex();
            } else {
                return 1L;
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        try {
            if (this.indexDB.getLastLogIndex() >= 0) {
                // Just use indexDB to get lastLogIndex
                return this.indexDB.getLastLogIndex();
            } else {
                return 0;
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();
        try {
            if (index < getFirstLogIndex() || index > getLastLogIndex()) {
                return null;
            }
            final IndexEntry indexEntry = this.indexDB.lookupIndex(index);
            final int phyPosition = indexEntry.getPosition();
            final byte logType = indexEntry.getLogType();
            if (phyPosition != -1) {
                byte[] logBytes;
                if (logType == IndexType.IndexSegment.getType()) {
                    logBytes = this.segmentLogDB.lookupLog(index, phyPosition);
                } else {
                    logBytes = this.confDB.lookupLog(index, phyPosition);
                }
                if (logBytes != null) {
                    return this.logEntryDecoder.decode(logBytes);
                }
            }
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    @Override
    public long getTerm(final long index) {
        final LogEntry entry = getEntry(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return 0;
    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
        this.readLock.lock();
        try {
            final long logIndex = entry.getId().getIndex();
            final byte[] logData = this.logEntryEncoder.encode(entry);
            if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                return doAppendEntry(logIndex, logData, this.confDB, IndexType.IndexConf, true);
            } else {
                return doAppendEntry(logIndex, logData, this.segmentLogDB, IndexType.IndexSegment, true);
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
        this.readLock.lock();
        try {
            int appendCount = 0;
            final int size = entries.size();

            // Find last log and last conf log
            int lastLogIndex = -1;
            int lastConfIndex = -1;
            for (int i = entries.size() - 1; i >= 0; i--) {
                final LogEntry entry = entries.get(i);
                final boolean isConfEntry = (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION);
                if (isConfEntry && lastConfIndex == -1) {
                    lastConfIndex = i;
                } else if (!isConfEntry && lastLogIndex == -1) {
                    lastLogIndex = i;
                }
                if (lastConfIndex >= 0 && lastLogIndex >= 0) {
                    break;
                }
            }

            for (int i = 0; i < size; i++) {
                final boolean isWaitingFlush = (i == lastLogIndex || i == lastConfIndex);
                final LogEntry entry = entries.get(i);
                final long logIndex = entry.getId().getIndex();
                final byte[] logData = this.logEntryEncoder.encode(entry);
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    if (doAppendEntry(logIndex, logData, this.confDB, IndexType.IndexConf, isWaitingFlush)) {
                        appendCount++;
                    }
                } else {
                    if (doAppendEntry(logIndex, logData, this.segmentLogDB, IndexType.IndexSegment, isWaitingFlush)) {
                        appendCount++;
                    }
                }
            }

            return appendCount;
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean doAppendEntry(final long logIndex, final byte[] data, final AbstractDB logDB,
                                  final IndexType indexType, final boolean isWaitingFlush) {
        this.readLock.lock();
        try {
            if (logDB == null || this.indexDB == null) {
                return false;
            }

            // Append log async , get position infos
            final Pair<Integer, Long> logPair = logDB.appendLogAsync(logIndex, data);
            if (logPair.getFirst() < 0 || logPair.getSecond() < 0) {
                return false;
            }

            final Pair<Integer, Long> indexPair = this.indexDB
                .appendIndexAsync(logIndex, logPair.getFirst(), indexType);
            if (indexPair.getFirst() < 0 || indexPair.getSecond() < 0) {
                return false;
            }

            // Save first log index
            if (!this.firstLogIndexCheckpoint.isInit()) {
                saveFirstLogIndex(logIndex);
            }

            if (isWaitingFlush) {
                return waitForFlush(logDB, logPair.getSecond(), indexPair.getSecond());
            }
            return true;
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean waitForFlush(final AbstractDB logDB, final long exceptedLogPosition,
                                 final long exceptedIndexPosition) {
        final int maxFlushTimes = this.storeOptions.getMaxFlushTimes();
        // We should flush log db first, because even If the power fails after flushing the log db
        // we can restore the index db based on the log db.
        if (!logDB.waitForFlush(exceptedLogPosition, maxFlushTimes)) {
            return false;
        }
        return this.indexDB.waitForFlush(exceptedIndexPosition, maxFlushTimes);
    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        this.readLock.lock();
        try {
            final boolean ret = saveFirstLogIndex(firstIndexKept);
            if (ret) {
                Utils.runInThread(() -> {
                    this.indexDB.truncatePrefix(firstIndexKept);
                    this.segmentLogDB.truncatePrefix(firstIndexKept);
                    this.confDB.truncatePrefix(firstIndexKept);
                });
            }
            return ret;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        final Pair<Integer, Integer> posPair = this.indexDB.lookupFirstLogPosFromLogIndex(lastIndexKept + 1);
        final int SegmentTruncatePos = posPair.getFirst();
        final int ConfLogTruncatePos = posPair.getSecond();
        final int lastIndexKeptPos = this.indexDB.lookupIndex(lastIndexKept).getPosition();

        if (lastIndexKeptPos != -1) {
            // Truncate indexDB
            this.indexDB.truncateSuffix(lastIndexKept, 0);
            // Truncate segmentDB
            this.segmentLogDB.truncateSuffix(lastIndexKept, SegmentTruncatePos);
            // Truncate confDB
            this.confDB.truncateSuffix(lastIndexKept, ConfLogTruncatePos);

            return this.indexDB.getLastLogIndex() == lastIndexKept;
        }

        return false;
    }

    @Override
    public boolean reset(final long nextLogIndex) {
        this.writeLock.lock();
        try {
            LogEntry entry = getEntry(nextLogIndex);
            this.indexDB.reset(nextLogIndex);
            this.segmentLogDB.reset(nextLogIndex);
            this.confDB.reset(nextLogIndex);
            if (entry == null) {
                entry = new LogEntry();
                entry.setType(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
                entry.setId(new LogId(nextLogIndex, 0));
            }
            saveFirstLogIndex(-1);
            return appendEntry(entry);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            this.indexDB.shutdown();
            this.segmentLogDB.shutdown();
            this.confDB.shutdown();
        } catch (final Exception e) {
            LOG.error("Error on shutdown dbs", e);
        } finally {
            this.writeLock.unlock();
        }
    }

    @OnlyForTest
    public IndexDB getIndexDB() {
        return indexDB;
    }

    @OnlyForTest
    public ConfDB getConfDB() {
        return confDB;
    }

    @OnlyForTest
    public SegmentLogDB getSegmentLogDB() {
        return segmentLogDB;
    }
}
