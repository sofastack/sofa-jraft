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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alipay.sofa.jraft.util.*;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;

/**
 * Log storage based on rocksdb.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-06 7:27:47 AM
 */
public class RocksDBLogStorage implements LogStorage, Describer {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBLogStorage.class);

    static {
        RocksDB.loadLibrary();
    }

    /**
     * Write batch template.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2017-Nov-08 11:19:22 AM
     */
    private interface WriteBatchTemplate {

        void execute(WriteBatch batch) throws RocksDBException, IOException, InterruptedException;
    }

    /**
     * A write context
     * @author boyan(boyan@antfin.com)
     *
     */
    public interface WriteContext {
        /**
         * Start a sub job.
         */
        default void startJob() {
        }

        /**
         * Finish a sub job
         */
        default void finishJob() {
        }

        /**
         * Adds a callback that will be invoked after all sub jobs finish.
         */
        default void addFinishHook(final Runnable r) {

        }

        /**
         * Set an exception to context.
         * @param e exception
         */
        default void setError(final Exception e) {
        }

        /**
         * Wait for all sub jobs finish.
         */
        default void joinAll() throws InterruptedException, IOException {
        }
    }

    /**
     * An empty write context
     * @author boyan(boyan@antfin.com)
     *
     */
    protected static class EmptyWriteContext implements WriteContext {
        static EmptyWriteContext INSTANCE = new EmptyWriteContext();
    }

    public static final String PART_ROCKSDB_OPTIONS_KEY = "jraft.log_storage.rocksdb_opts.per_group";

    private String groupId;
    private boolean partRocksDBOptions;
    private final String                    path;
    private final boolean                   sync;
    private final boolean                   openStatistics;
    private RocksDB                         db;
    private DBOptions                       dbOptions;
    private WriteOptions                    writeOptions;
    private final List<ColumnFamilyOptions> cfOptions     = new ArrayList<>();
    private ColumnFamilyHandle              defaultHandle;
    private ColumnFamilyHandle              confHandle;
    private ReadOptions                     totalOrderReadOptions;
    private DebugStatistics                 statistics;
    private final ReadWriteLock             readWriteLock = new ReentrantReadWriteLock();
    private final Lock                      readLock      = this.readWriteLock.readLock();
    private final Lock                      writeLock     = this.readWriteLock.writeLock();

    private volatile long                   firstLogIndex = 1;

    private volatile boolean                hasLoadFirstLogIndex;

    private LogEntryEncoder                 logEntryEncoder;
    private LogEntryDecoder                 logEntryDecoder;

    public RocksDBLogStorage(final String path, final RaftOptions raftOptions) {
        super();
        this.path = path;
        this.sync = raftOptions.isSync();
        this.openStatistics = raftOptions.isOpenStatistics();
    }

    private String getRocksDBGroup() {
        return partRocksDBOptions ? this.groupId : null;
    }

    private DBOptions createDBOptions() {
        return StorageOptionsFactory.getRocksDBOptions(getRocksDBGroup(), RocksDBLogStorage.class);
    }

    private ColumnFamilyOptions createColumnFamilyOptions() {
        final BlockBasedTableConfig tConfig = StorageOptionsFactory
                .getRocksDBTableFormatConfig(getRocksDBGroup(), RocksDBLogStorage.class);
        return StorageOptionsFactory
                .getRocksDBColumnFamilyOptions(getRocksDBGroup(), RocksDBLogStorage.class) //
                .useFixedLengthPrefixExtractor(8) //
                .setTableFormatConfig(tConfig) //
                .setMergeOperator(new StringAppendOperator());
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        Requires.requireNonNull(opts.getConfigurationManager(), "Null conf manager");
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        this.groupId = opts.getGroupId();
        this.writeLock.lock();
        try {
            this.partRocksDBOptions = SystemPropertyUtil.getBoolean(PART_ROCKSDB_OPTIONS_KEY, false);
            if (this.db != null) {
                LOG.warn("RocksDBLogStorage init() in {} already.", this.path);
                return true;
            }
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            Requires.requireNonNull(this.logEntryDecoder, "Null log entry decoder");
            Requires.requireNonNull(this.logEntryEncoder, "Null log entry encoder");
            this.dbOptions = createDBOptions();
            if (this.openStatistics) {
                this.statistics = new DebugStatistics();
                this.dbOptions.setStatistics(this.statistics);
            }

            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(this.sync);
            this.totalOrderReadOptions = new ReadOptions();
            this.totalOrderReadOptions.setTotalOrderSeek(true);

            return initAndLoad(opts.getConfigurationManager());
        } catch (final RocksDBException e) {
            LOG.error("Fail to init RocksDBLogStorage, path={}.", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }

    }

    private boolean initAndLoad(final ConfigurationManager confManager) throws RocksDBException {
        this.hasLoadFirstLogIndex = false;
        this.firstLogIndex = 1;
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        final ColumnFamilyOptions cfOption = createColumnFamilyOptions();
        this.cfOptions.add(cfOption);
        // Column family to store configuration log entry.
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("Configuration".getBytes(), cfOption));
        // Default column family to store user data log entry.
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOption));

        openDB(columnFamilyDescriptors);
        load(confManager);
        return onInitLoaded();
    }

    /**
     * First log index and last log index key in configuration column family.
     */
    public static final byte[] FIRST_LOG_IDX_KEY = Utils.getBytes("meta/firstLogIndex");

    private void load(final ConfigurationManager confManager) {
        checkState();
        try (final RocksIterator it = this.db.newIterator(this.confHandle, this.totalOrderReadOptions)) {
            it.seekToFirst();
            while (it.isValid()) {
                final byte[] ks = it.key();
                final byte[] bs = it.value();

                // LogEntry index
                if (ks.length == 8) {
                    final LogEntry entry = this.logEntryDecoder.decode(bs);
                    if (entry != null) {
                        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                            final ConfigurationEntry confEntry = new ConfigurationEntry();
                            confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                            confEntry.setConf(new Configuration(entry.getPeers(), entry.getLearners()));
                            if (entry.getOldPeers() != null) {
                                confEntry.setOldConf(new Configuration(entry.getOldPeers(), entry.getOldLearners()));
                            }
                            if (confManager != null) {
                                confManager.add(confEntry);
                            }
                        }
                    } else {
                        LOG.warn("Fail to decode conf entry at index {}, the log data is: {}.", Bits.getLong(ks, 0),
                            BytesUtil.toHex(bs));
                    }
                } else {
                    if (Arrays.equals(FIRST_LOG_IDX_KEY, ks)) {
                        setFirstLogIndex(Bits.getLong(bs, 0));
                        truncatePrefixInBackground(0L, this.firstLogIndex);
                    } else {
                        LOG.warn("Unknown entry in configuration storage key={}, value={}.", BytesUtil.toHex(ks),
                            BytesUtil.toHex(bs));
                    }
                }
                it.next();
            }
        }
    }

    private void setFirstLogIndex(final long index) {
        this.firstLogIndex = index;
        this.hasLoadFirstLogIndex = true;
    }

    /**
     * Save the first log index into conf column family.
     */
    private boolean saveFirstLogIndex(final long firstLogIndex) {
        this.readLock.lock();
        try {
            final byte[] vs = new byte[8];
            Bits.putLong(vs, 0, firstLogIndex);
            checkState();
            this.db.put(this.confHandle, this.writeOptions, FIRST_LOG_IDX_KEY, vs);
            return true;
        } catch (final RocksDBException e) {
            LOG.error("Fail to save first log index {} in {}.", firstLogIndex, this.path, e);
            return false;
        } finally {
            this.readLock.unlock();
        }
    }

    private void openDB(final List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws RocksDBException {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        final File dir = new File(this.path);
        if (dir.exists() && !dir.isDirectory()) {
            throw new IllegalStateException("Invalid log path, it's a regular file: " + this.path);
        }
        this.db = RocksDB.open(this.dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles);

        assert (columnFamilyHandles.size() == 2);
        this.confHandle = columnFamilyHandles.get(0);
        this.defaultHandle = columnFamilyHandles.get(1);
    }

    private void checkState() {
        Requires.requireNonNull(this.db, "DB not initialized or destroyed");
    }

    /**
     * Execute write batch template.
     *
     * @param template write batch template
     */
    private boolean executeBatch(final WriteBatchTemplate template) {
        this.readLock.lock();
        if (this.db == null) {
            LOG.warn("DB not initialized or destroyed in data path: {}.", this.path);
            this.readLock.unlock();
            return false;
        }
        try (final WriteBatch batch = new WriteBatch()) {
            template.execute(batch);
            this.db.write(this.writeOptions, batch);
        } catch (final RocksDBException e) {
            LOG.error("Execute batch failed with rocksdb exception.", e);
            return false;
        } catch (final IOException e) {
            LOG.error("Execute batch failed with io exception.", e);
            return false;
        } catch (final InterruptedException e) {
            LOG.error("Execute batch failed with interrupt.", e);
            Thread.currentThread().interrupt();
            return false;
        } finally {
            this.readLock.unlock();
        }
        return true;
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            // The shutdown order is matter.
            // 1. close column family handles
            closeDB();
            onShutdown();
            // 2. close column family options.
            for (final ColumnFamilyOptions opt : this.cfOptions) {
                opt.close();
            }
            // 3. close options
            this.dbOptions.close();
            if (this.statistics != null) {
                this.statistics.close();
            }
            this.writeOptions.close();
            this.totalOrderReadOptions.close();
            // 4. help gc.
            this.cfOptions.clear();
            this.dbOptions = null;
            this.statistics = null;
            this.writeOptions = null;
            this.totalOrderReadOptions = null;
            this.defaultHandle = null;
            this.confHandle = null;
            this.db = null;
            LOG.info("DB destroyed, the db path is: {}.", this.path);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void closeDB() {
        this.confHandle.close();
        this.defaultHandle.close();
        this.db.close();
    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        RocksIterator it = null;
        try {
            if (this.hasLoadFirstLogIndex) {
                return this.firstLogIndex;
            }
            checkState();
            it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions);
            it.seekToFirst();
            if (it.isValid()) {
                final long ret = Bits.getLong(it.key(), 0);
                saveFirstLogIndex(ret);
                setFirstLogIndex(ret);
                return ret;
            }
            return 1L;
        } finally {
            if (it != null) {
                it.close();
            }
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        checkState();
        try (final RocksIterator it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions)) {
            it.seekToLast();
            if (it.isValid()) {
                return Bits.getLong(it.key(), 0);
            }
            return 0L;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
                return null;
            }
            return getEntryFromDB(index);
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to get log entry at index {} in data path: {}.", index, this.path, e);
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    @OnlyForTest
    LogEntry getEntryFromDB(final long index) throws IOException, RocksDBException {
      final byte[] keyBytes = getKeyBytes(index);
      final byte[] bs = onDataGet(index, getValueFromRocksDB(keyBytes));
      if (bs != null) {
          final LogEntry entry = this.logEntryDecoder.decode(bs);
          if (entry != null) {
              return entry;
          } else {
              LOG.error("Bad log entry format for index={}, the log data is: {}.", index, BytesUtil.toHex(bs));
              // invalid data remove? TODO
              return null;
          }
      }
      return null;
    }

    protected byte[] getValueFromRocksDB(final byte[] keyBytes) throws RocksDBException {
        checkState();
        return this.db.get(this.defaultHandle, keyBytes);
    }

    protected byte[] getKeyBytes(final long index) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, index);
        return ks;
    }

    @Override
    public long getTerm(final long index) {
        final LogEntry entry = getEntry(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return 0;
    }

    private void addConfBatch(final LogEntry entry, final WriteBatch batch) throws RocksDBException {
        final byte[] ks = getKeyBytes(entry.getId().getIndex());
        final byte[] content = this.logEntryEncoder.encode(entry);
        batch.put(this.defaultHandle, ks, content);
        batch.put(this.confHandle, ks, content);
    }

    private void addDataBatch(final LogEntry entry, final WriteBatch batch,
                              final WriteContext ctx) throws RocksDBException, IOException, InterruptedException {
        final long logIndex = entry.getId().getIndex();
        final byte[] content = this.logEntryEncoder.encode(entry);
        batch.put(this.defaultHandle, getKeyBytes(logIndex), onDataAppend(logIndex, content, ctx));
    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
            return executeBatch(batch -> addConfBatch(entry, batch));
        } else {
            this.readLock.lock();
            try {
                if (this.db == null) {
                    LOG.warn("DB not initialized or destroyed in data path: {}.", this.path);
                    return false;
                }
                final WriteContext writeCtx = newWriteContext();
                final long logIndex = entry.getId().getIndex();
                final byte[] valueBytes = this.logEntryEncoder.encode(entry);
                final byte[] newValueBytes = onDataAppend(logIndex, valueBytes, writeCtx);
                writeCtx.startJob();
                this.db.put(this.defaultHandle, this.writeOptions, getKeyBytes(logIndex), newValueBytes);
                writeCtx.joinAll();
                if (newValueBytes != valueBytes) {
                    doSync();
                }
                return true;
            } catch (final RocksDBException | IOException e) {
                LOG.error("Fail to append entry.", e);
                return false;
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                this.readLock.unlock();
            }
        }
    }

    private void doSync() throws IOException, InterruptedException {
        onSync();
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        final int entriesCount = entries.size();
        final boolean ret = executeBatch(batch -> {
            final WriteContext writeCtx = newWriteContext();
            for (int i = 0; i < entriesCount; i++) {
                final LogEntry entry = entries.get(i);
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    addConfBatch(entry, batch);
                } else {
                    writeCtx.startJob();
                    addDataBatch(entry, batch, writeCtx);
                }
            }
            writeCtx.joinAll();
            doSync();
        });

        if (ret) {
            return entriesCount;
        } else {
            return 0;
        }
    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        this.readLock.lock();
        try {
            final long startIndex = getFirstLogIndex();
            final boolean ret = saveFirstLogIndex(firstIndexKept);
            if (ret) {
                setFirstLogIndex(firstIndexKept);
            }
            truncatePrefixInBackground(startIndex, firstIndexKept);
            return ret;
        } finally {
            this.readLock.unlock();
        }

    }

    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept) {
        // delete logs in background.
        ThreadPoolsFactory.runInThread(this.groupId, () -> {
            long startMs = Utils.monotonicMs();
            this.readLock.lock();
            try {
                RocksDB db = this.db;
                if (db == null) {
                    LOG.warn(
                        "DB is null while truncating prefixed logs in data path: {}, the range is: [{}, {})",
                        this.path, startIndex, firstIndexKept);
                    return;
                }
                onTruncatePrefix(startIndex, firstIndexKept);
                // Note https://github.com/facebook/rocksdb/wiki/Delete-A-Range-Of-Keys
                final byte[] startKey = getKeyBytes(startIndex);
                final byte[] endKey = getKeyBytes(firstIndexKept);
                // deleteRange to delete all keys in range.
                db.deleteRange(this.defaultHandle, startKey, endKey);
                db.deleteRange(this.confHandle, startKey, endKey);
                // deleteFilesInRanges to speedup reclaiming disk space on write-heavy load.
                db.deleteFilesInRanges(this.defaultHandle, Arrays.asList(startKey, endKey), false);
                db.deleteFilesInRanges(this.confHandle, Arrays.asList(startKey, endKey), false);
            } catch (final RocksDBException | IOException e) {
                LOG.error("Fail to truncatePrefix in data path: {}, firstIndexKept={}.", this.path, firstIndexKept, e);
            } finally {
                this.readLock.unlock();
                LOG.info("Truncated prefix logs in data path: {} from log index {} to {}, cost {} ms.",
                    this.path, startIndex, firstIndexKept, Utils.monotonicMs() - startMs);
            }
        });
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        this.readLock.lock();
        try {
            try {
                onTruncateSuffix(lastIndexKept);
            } finally {
                this.db.deleteRange(this.defaultHandle, this.writeOptions, getKeyBytes(lastIndexKept + 1),
                    getKeyBytes(getLastLogIndex() + 1));
                this.db.deleteRange(this.confHandle, this.writeOptions, getKeyBytes(lastIndexKept + 1),
                    getKeyBytes(getLastLogIndex() + 1));
            }
            return true;
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to truncateSuffix {} in data path: {}.", lastIndexKept, this.path, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    @Override
    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.writeLock.lock();
        try (final Options opt = new Options()) {
            LogEntry entry = getEntry(nextLogIndex);
            closeDB();
            try {
                RocksDB.destroyDB(this.path, opt);
                onReset(nextLogIndex);
                if (initAndLoad(null)) {
                    if (entry == null) {
                        entry = new LogEntry();
                        entry.setType(EntryType.ENTRY_TYPE_NO_OP);
                        entry.setId(new LogId(nextLogIndex, 0));
                        LOG.warn("Entry not found for nextLogIndex {} when reset in data path: {}.", nextLogIndex, this.path);
                    }
                    return appendEntry(entry);
                } else {
                    return false;
                }
            } catch (final RocksDBException e) {
                LOG.error("Fail to reset next log index.", e);
                return false;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    // Hooks for {@link RocksDBSegmentLogStorage}

    /**
     * Called after opening RocksDB and loading configuration into conf manager.
     */
    protected boolean onInitLoaded() {
        return true;
    }

    /**
     * Called after closing db.
     */
    protected void onShutdown() {
    }

    /**
     * Called after resetting db.
     *
     * @param nextLogIndex next log index
     */
    protected void onReset(final long nextLogIndex) {
    }

    /**
     * Called after truncating prefix logs in rocksdb.
     *
     * @param startIndex     the start index
     * @param firstIndexKept the first index to kept
     */
    protected void onTruncatePrefix(final long startIndex, final long firstIndexKept) throws RocksDBException,
    IOException {
    }

    /**
     * Called when sync data into file system.
     */
    protected void onSync() throws IOException, InterruptedException {
    }

    protected boolean isSync() {
        return this.sync;
    }

    /**
     * Called after truncating suffix logs in rocksdb.
     *
     * @param lastIndexKept the last index to kept
     */
    protected void onTruncateSuffix(final long lastIndexKept) throws RocksDBException, IOException {
    }

    protected WriteContext newWriteContext() {
        return EmptyWriteContext.INSTANCE;
    }

    /**
     * Called before appending data entry.
     *
     * @param logIndex the log index
     * @param value    the data value in log entry.
     * @return the new value
     */
    protected byte[] onDataAppend(final long logIndex, final byte[] value,
                                  final WriteContext ctx) throws IOException, InterruptedException {
        ctx.finishJob();
        return value;
    }

    /**
     * Called after getting data from rocksdb.
     *
     * @param logIndex the log index
     * @param value    the value in rocksdb
     * @return the new value
     */
    protected byte[] onDataGet(final long logIndex, final byte[] value) throws IOException {
        return value;
    }

    @Override
    public void describe(final Printer out) {
        this.readLock.lock();
        try {
            if (this.db != null) {
                out.println(this.db.getProperty("rocksdb.stats"));
            }
            out.println("");
            if (this.statistics != null) {
                out.println(this.statistics.getString());
            }
        } catch (final RocksDBException e) {
            out.println(e);
        } finally {
            this.readLock.unlock();
        }
    }
}
