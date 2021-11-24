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
package com.alipay.sofa.jraft.logStore;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.logStore.db.AbstractDB;
import com.alipay.sofa.jraft.logStore.db.ConfDB;
import com.alipay.sofa.jraft.logStore.db.ConfDB.ConfIterator;
import com.alipay.sofa.jraft.logStore.db.IndexDB;
import com.alipay.sofa.jraft.logStore.db.SegmentLogDB;
import com.alipay.sofa.jraft.logStore.factory.LogStoreFactory;
import com.alipay.sofa.jraft.logStore.file.assit.FirstLogIndexCheckpoint;
import com.alipay.sofa.jraft.logStore.file.index.IndexFile.IndexEntry;
import com.alipay.sofa.jraft.logStore.file.index.IndexType;
import com.alipay.sofa.jraft.logStore.service.FlushRequest;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.Pair;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hzh (642256541@qq.com)
 */
public class DefaultLogStorage implements LogStorage {
    private static final Logger           LOG                    = LoggerFactory.getLogger(DefaultLogStorage.class);

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

    public DefaultLogStorage(final String path, final StoreOptions storeOptions) {
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
                return false;
            }

            this.firstLogIndexCheckpoint.load();
            return recoverAndLoad();
        } catch (final IOException ignored) {
            LOG.error("Error on load firstLogIndexCheckPoint");
        } finally {
            this.writeLock.unlock();
        }
        return false;
    }

    public boolean recoverAndLoad() {
        this.writeLock.lock();
        try {
            // DB recover takes a lot of time and needs to run in parallel
            final CompletableFuture<Void> indexRecover = CompletableFuture.runAsync(() -> {
                this.indexDB.recover();
            }, Utils.getClosureExecutor());

            final CompletableFuture<Void> segmentRecover = CompletableFuture.runAsync(() -> {
                this.segmentLogDB.recover();
            }, Utils.getClosureExecutor());

            final CompletableFuture<Void> confRecover = CompletableFuture.runAsync(() -> {
                this.confDB.recover();
                loadConfiguration();
            }, Utils.getClosureExecutor());

            // Wait for recover
            final int recoverDBTimeout = this.storeOptions.getRecoverDBTimeout();
            CompletableFuture.allOf(indexRecover, segmentRecover, confRecover).get(recoverDBTimeout, TimeUnit.MILLISECONDS);

            // Set first log index
            if (!this.firstLogIndexCheckpoint.isInit()) {
                saveFirstLogIndex(this.indexDB.getFirstLogIndex());
            }

            LOG.info("Recover dbs and start timingServer success, last recover index:{}", this.indexDB.getLastLogIndex());
            return true;
        } catch (final Exception e) {
            LOG.error("Error on recover db", e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
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
        final ConfIterator confIterator = this.confDB.Iterator(this.logEntryDecoder);
        LogEntry entry;
        while ((entry = confIterator.next()) != null) {
            if (entry.isConfigurationEntry()) {
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
            if (entry.isConfigurationEntry()) {
                return doAppendEntryAsync(logIndex, logData, this.confDB, IndexType.IndexConf, true);
            } else {
                return doAppendEntryAsync(logIndex, logData, this.segmentLogDB, IndexType.IndexSegment, true);
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
                final boolean isConfEntry = entry.isConfigurationEntry();
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
                if (entry.isConfigurationEntry()) {
                    if (doAppendEntryAsync(logIndex, logData, this.confDB, IndexType.IndexConf, isWaitingFlush)) {
                        appendCount++;
                    }
                } else {
                    if (doAppendEntryAsync(logIndex, logData, this.segmentLogDB, IndexType.IndexSegment, isWaitingFlush)) {
                        appendCount++;
                    }
                }
            }

            return appendCount;
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean doAppendEntryAsync(final long logIndex, final byte[] data, final AbstractDB logDB,
                                       final IndexType indexType, final boolean isWaitingFlush) {
        this.readLock.lock();
        try {
            if (logDB == null || this.indexDB == null) {
                LOG.warn("DB not initialized or destroyed");
                return false;
            }

            // Append log async , get position infos
            final Pair<Integer, Long> logPair = logDB.appendLogAsync(logIndex, data);
            final Pair<Integer, Long> indexPair = this.indexDB.appendIndexAsync(logIndex, logPair.getKey(), indexType);

            // Save first log index
            if (!this.firstLogIndexCheckpoint.isInit()) {
                saveFirstLogIndex(logIndex);
            }

            // Group commit, register a flush request to flushService, wait for flush done
            if (isWaitingFlush) {
                return waitForFlush(logDB, logPair.getValue(), indexPair.getValue());
            }

            return true;
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean waitForFlush(final AbstractDB logDB, final long exceptedLogPosition,
                                 final long exceptedIndexPosition) {
        try {
            final FlushRequest logRequest = FlushRequest.buildRequest(exceptedLogPosition);
            final FlushRequest indexRequest = FlushRequest.buildRequest(exceptedIndexPosition);
            logDB.registerFlushRequest(logRequest);
            this.indexDB.registerFlushRequest(indexRequest);

            final int timeout = this.storeOptions.getWaitingFlushTimeout();
            CompletableFuture.allOf(logRequest.getFuture(), indexRequest.getFuture()).get(timeout,
                TimeUnit.MILLISECONDS);
            return true;
        } catch (final Exception e) {
            LOG.error(
                "Timeout when wait flush request, current log pos:{}, expected log flush pos:{}, current index pos:{},"
                        + "expected index flush pos:{}", logDB.getFlushedPosition(), exceptedLogPosition,
                this.indexDB.getFlushedPosition(), exceptedIndexPosition, e);
            return false;
        }
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
        final int SegmentTruncatePos = posPair.getKey();
        final int ConfLogTruncatePos = posPair.getValue();
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

            // Shutdown takes a lot of time and needs to run in parallel
            final CompletableFuture<Void> indexShutdown = CompletableFuture.runAsync(() -> {
                this.indexDB.shutdown();
            }, Utils.getClosureExecutor());

            final CompletableFuture<Void> segmentShutdown = CompletableFuture.runAsync(() -> {
                this.segmentLogDB.shutdown();
            }, Utils.getClosureExecutor());

            final CompletableFuture<Void> confShutdown = CompletableFuture.runAsync(() -> {
                this.confDB.shutdown();
            }, Utils.getClosureExecutor());

            // Wait for shutdown
            final int shutdownDBTimeout = this.storeOptions.getShutdownDBTimeout();
            CompletableFuture.allOf(indexShutdown, segmentShutdown, confShutdown).get(shutdownDBTimeout, TimeUnit.MILLISECONDS);

            LOG.info("Shutdown dbs success");
        } catch (final Exception e) {
            LOG.error("Error on shutdown dbs", e);
        } finally {
            this.writeLock.unlock();
        }
    }
}
