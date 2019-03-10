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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.IndexType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.StorageOptionsFactory;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Log storage based on rocksdb.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-06 7:27:47 AM
 */
public class RocksDBLogStorage implements LogStorage {

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

        void execute(WriteBatch batch) throws RocksDBException;
    }

    private final String                    path;
    private final boolean                   sync;
    private RocksDB                         db;
    private DBOptions                       dbOptions;
    private WriteOptions                    writeOptions;
    private final List<ColumnFamilyOptions> cfOptions     = new ArrayList<>();
    private ColumnFamilyHandle              defaultHandle;
    private ColumnFamilyHandle              confHandle;
    private ReadOptions                     totalOrderReadOptions;
    private final ReadWriteLock             lock          = new ReentrantReadWriteLock(false);
    private final Lock                      readLock      = lock.readLock();
    private final Lock                      writeLock     = lock.writeLock();

    private volatile long                   firstLogIndex = 1;

    private volatile boolean                hasLoadFirstLogIndex;

    public RocksDBLogStorage(String path, RaftOptions raftOptions) {
        super();
        this.path = path;
        this.sync = raftOptions.isSync();
    }

    private static BlockBasedTableConfig createTableConfig() {
        return new BlockBasedTableConfig(). //
            setIndexType(IndexType.kHashSearch). // use hash search(btree) for prefix scan.
            setBlockSize(4 * SizeUnit.KB).//
            setFilter(new BloomFilter(16, false)). //
            setCacheIndexAndFilterBlocks(true). //
            setBlockCacheSize(512 * SizeUnit.MB). //
            setCacheNumShardBits(8);
    }

    public static DBOptions createDBOptions() {
        return StorageOptionsFactory.getRocksDBOptions(RocksDBLogStorage.class);
    }

    public static ColumnFamilyOptions createColumnFamilyOptions() {
        final BlockBasedTableConfig tConfig = createTableConfig();
        final ColumnFamilyOptions options = StorageOptionsFactory
            .getRocksDBColumnFamilyOptions(RocksDBLogStorage.class);
        return options.useFixedLengthPrefixExtractor(8). //
            setTableFormatConfig(tConfig). //
            setMergeOperator(new StringAppendOperator());
    }

    @Override
    public boolean init(ConfigurationManager confManager) {
        writeLock.lock();
        try {
            if (this.db != null) {
                LOG.warn("RocksDBLogStorage init() already.");
                return true;
            }
            this.dbOptions = createDBOptions();

            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(this.sync);
            this.totalOrderReadOptions = new ReadOptions();
            this.totalOrderReadOptions.setTotalOrderSeek(true);

            return initAndLoad(confManager);
        } catch (final RocksDBException e) {
            LOG.error("Fail to init RocksDBLogStorage, path={}", this.path, e);
            return false;
        } finally {
            writeLock.unlock();
        }

    }

    private boolean initAndLoad(ConfigurationManager confManager) throws RocksDBException {
        this.hasLoadFirstLogIndex = false;
        this.firstLogIndex = 1;
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        final ColumnFamilyOptions cfOption = createColumnFamilyOptions();
        this.cfOptions.add(cfOption);
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("Configuration".getBytes(), cfOption));
        // default column family
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOption));

        openDB(columnFamilyDescriptors);
        this.load(confManager);
        return true;
    }

    private void load(ConfigurationManager confManager) {
        try (RocksIterator it = this.db.newIterator(this.confHandle, this.totalOrderReadOptions)) {
            it.seekToFirst();
            while (it.isValid()) {
                final byte[] bs = it.value();
                final byte[] ks = it.key();

                // LogEntry index
                if (ks.length == 8) {
                    final LogEntry entry = new LogEntry();
                    if (entry.decode(bs)) {
                        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                            final ConfigurationEntry confEntry = new ConfigurationEntry();
                            confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                            confEntry.setConf(new Configuration(entry.getPeers()));
                            if (entry.getOldPeers() != null) {
                                confEntry.setOldConf(new Configuration(entry.getOldPeers()));
                            }
                            if (confManager != null) {
                                confManager.add(confEntry);
                            }
                        }
                    } else {
                        LOG.warn("Fail to decode conf entry at index {}", Bits.getLong(it.key(), 0));
                    }
                } else {
                    if (Arrays.equals(FIRST_LOG_IDX_KEY, ks)) {
                        setFirstLogIndex(Bits.getLong(bs, 0));
                        this.truncatePrefixInBackground(0L, this.firstLogIndex);
                    } else {
                        LOG.warn("Unknown entry in configuration storage key={}, value={}", Arrays.toString(ks),
                            Arrays.toString(bs));
                    }
                }
                it.next();
            }
        }
    }

    private void setFirstLogIndex(long index) {
        this.firstLogIndex = index;
        this.hasLoadFirstLogIndex = true;
    }

    /**
     * First log inex and last log index key in configuration column family.
     */
    public static final byte[] FIRST_LOG_IDX_KEY = Utils.getBytes("meta/firstLogIndex");

    /**
     * Save the first log index into conf column family.
     */
    private boolean saveFirstLogIndex(long firstLogIndex) {
        readLock.lock();
        try {
            final byte[] vs = new byte[8];
            Bits.putLong(vs, 0, firstLogIndex);
            this.db.put(this.confHandle, this.writeOptions, FIRST_LOG_IDX_KEY, vs);
            return true;
        } catch (final RocksDBException e) {
            LOG.error("Fail to save first log index {}", firstLogIndex, e);
            return false;
        } finally {
            readLock.unlock();
        }
    }

    private void openDB(final List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws RocksDBException {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        final File dir = new File(this.path);
        if (dir.exists() && !dir.isDirectory()) {
            throw new IllegalStateException("Invalid log path, it's a regular file: " + this.path);
        }
        this.db = RocksDB.open(this.dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles);

        this.confHandle = columnFamilyHandles.get(0);
        this.defaultHandle = columnFamilyHandles.get(1);
    }

    /**
     * Execute write batch template.
     *
     * @param template write batch template
     */
    private boolean executeBatch(WriteBatchTemplate template) {
        readLock.lock();
        try (final WriteBatch batch = new WriteBatch()) {
            template.execute(batch);
            this.db.write(this.writeOptions, batch);
        } catch (final RocksDBException e) {
            LOG.error("Execute rocksdb operation failed", e);
            return false;
        } finally {
            readLock.unlock();
        }
        return true;
    }

    @Override
    public void shutdown() {
        writeLock.lock();
        try {
            // The shutdown order is matter.
            // 1. close column family handles
            closeDB();
            // 2. close column family options.
            for (final ColumnFamilyOptions opt : this.cfOptions) {
                opt.close();
            }
            // 3. close options
            this.writeOptions.close();
            this.totalOrderReadOptions.close();
            // 4. help gc.
            this.cfOptions.clear();
            this.db = null;
            this.totalOrderReadOptions = null;
            this.writeOptions = null;
            this.defaultHandle = null;
            this.confHandle = null;
        } finally {
            writeLock.unlock();
        }
    }

    private void closeDB() {
        this.confHandle.close();
        this.defaultHandle.close();
        this.db.close();
    }

    @Override
    public long getFirstLogIndex() {
        readLock.lock();
        RocksIterator it = null;
        try {
            if (hasLoadFirstLogIndex) {
                return this.firstLogIndex;
            }
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
            readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        readLock.lock();
        try (final RocksIterator it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions)) {
            it.seekToLast();
            if (it.isValid()) {
                return Bits.getLong(it.key(), 0);
            }
            return 0L;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(long index) {
        readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
                return null;
            }
            final byte[] bs = this.db.get(this.defaultHandle, getKeyBytes(index));
            if (bs != null) {
                final LogEntry entry = new LogEntry();
                if (entry.decode(bs)) {
                    return entry;
                } else {
                    LOG.error("Bad log entry format for index={}", index);
                    // invalid data remove? TODO
                    return null;
                }
            }
        } catch (final RocksDBException e) {
            LOG.error("Fail to get log entry at index {}", index, e);
            return null;
        } finally {
            readLock.unlock();
        }
        return null;
    }

    private byte[] getKeyBytes(long index) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, index);
        return ks;
    }

    @Override
    public long getTerm(long index) {
        final LogEntry entry = getEntry(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return 0;
    }

    private void addConfBatch(LogEntry entry, WriteBatch batch) throws RocksDBException {
        final byte[] ks = getKeyBytes(entry.getId().getIndex());
        final byte[] content = entry.encode();
        batch.put(defaultHandle, ks, content);
        batch.put(confHandle, ks, content);
    }

    private void addDataBatch(LogEntry entry, WriteBatch batch) throws RocksDBException {
        final byte[] ks = getKeyBytes(entry.getId().getIndex());
        final byte[] content = entry.encode();
        batch.put(defaultHandle, ks, content);
    }

    @Override
    public boolean appendEntry(LogEntry entry) {
        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
            return this.executeBatch(batch -> addConfBatch(entry, batch));

        } else {
            readLock.lock();
            try {
                this.db.put(this.defaultHandle, getKeyBytes(entry.getId().getIndex()), entry.encode());
                return true;
            } catch (final RocksDBException e) {
                LOG.error("Fail to append entry", e);
                return false;
            } finally {
                readLock.unlock();
            }
        }
    }

    @Override
    public int appendEntries(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        final int entriesCount = entries.size();
        final boolean ret = this.executeBatch(batch -> {
            for (int i = 0; i < entriesCount; i++) {
                final LogEntry entry = entries.get(i);
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    addConfBatch(entry, batch);
                } else {
                    addDataBatch(entry, batch);
                }
            }
        });

        if (ret) {
            return entriesCount;
        } else {
            return 0;
        }

    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        readLock.lock();
        try {
            final long startIndex = this.getFirstLogIndex();
            final boolean ret = this.saveFirstLogIndex(firstIndexKept);
            if (ret) {
                this.setFirstLogIndex(firstIndexKept);
            }
            truncatePrefixInBackground(startIndex, firstIndexKept);
            return ret;
        } finally {
            readLock.unlock();
        }

    }

    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept) {
        // delete logs in background.
        Utils.runInThread(() -> {
            readLock.lock();
            try {
                if (db == null) {
                    return;
                }
                db.deleteRange(defaultHandle, getKeyBytes(startIndex), getKeyBytes(firstIndexKept));
                db.deleteRange(confHandle, getKeyBytes(startIndex), getKeyBytes(firstIndexKept));
            } catch (final RocksDBException e) {
                LOG.error("Fail to truncatePrefix {}", firstIndexKept, e);
            } finally {
                readLock.unlock();
            }
        });
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept) {
        readLock.lock();
        try {
            db.deleteRange(defaultHandle, this.writeOptions, getKeyBytes(lastIndexKept + 1),
                getKeyBytes(getLastLogIndex() + 1));
            db.deleteRange(confHandle, this.writeOptions, getKeyBytes(lastIndexKept + 1),
                getKeyBytes(getLastLogIndex() + 1));
            return true;
        } catch (final RocksDBException e) {
            LOG.error("Fail to truncateSuffix {}", lastIndexKept, e);

        } finally {
            readLock.unlock();
        }
        return false;
    }

    @Override
    public boolean reset(long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        writeLock.lock();
        try (Options opt = new Options()) {
            LogEntry entry = this.getEntry(nextLogIndex);
            this.closeDB();
            try {
                RocksDB.destroyDB(path, opt);
                if (this.initAndLoad(null)) {
                    if (entry == null) {
                        entry = new LogEntry();
                        entry.setType(EntryType.ENTRY_TYPE_NO_OP);
                        entry.setId(new LogId(nextLogIndex, 0));
                        LOG.warn("Entry not found for nextLogIndex {} when reset", nextLogIndex);
                    }
                    return this.appendEntry(entry);
                } else {
                    return false;
                }
            } catch (final RocksDBException e) {
                LOG.error("Fail to reset next log index", e);
                return false;
            }
        } finally {
            writeLock.unlock();
        }
    }
}
