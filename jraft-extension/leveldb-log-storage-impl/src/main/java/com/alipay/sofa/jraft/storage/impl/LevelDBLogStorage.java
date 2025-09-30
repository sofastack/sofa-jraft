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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alipay.sofa.jraft.util.ThreadPoolsFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;
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
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Log storage based on leveldb.
 *
 * @author knightblood
 */
public class LevelDBLogStorage implements LogStorage, Describer {

    private static final Logger LOG                   = LoggerFactory.getLogger(LevelDBLogStorage.class);
    static final String         DEFAULT_DATABASE_NAME = "jraft-log";
    static final String         CONF_DATABASE_NAME    = "jraft-conf";

    private String              groupId;
    private DB                  defaultDB;
    private DB                  confDB;
    private final String        homePath;
    private boolean             opened                = false;

    private LogEntryEncoder     logEntryEncoder;
    private LogEntryDecoder     logEntryDecoder;

    private final ReadWriteLock readWriteLock         = new ReentrantReadWriteLock();
    private final Lock          readLock              = this.readWriteLock.readLock();
    private final Lock          writeLock             = this.readWriteLock.writeLock();

    private final boolean       sync;

    private volatile long       firstLogIndex         = 1;
    private volatile boolean    hasLoadFirstLogIndex;

    /**
     * First log index and last log index key in configuration column family.
     */
    public static final byte[]  FIRST_LOG_IDX_KEY     = Utils.getBytes("meta/firstLogIndex");

    public LevelDBLogStorage(final String homePath, final RaftOptions raftOptions) {
        super();
        Requires.requireNonNull(homePath, "Null homePath");
        this.homePath = homePath;
        this.sync = raftOptions.isSync();
    }

    @Override
    public boolean init(LogStorageOptions opts) {
        Requires.requireNonNull(opts, "Null LogStorageOptions opts");
        Requires.requireNonNull(opts.getConfigurationManager(), "Null conf manager");
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        this.groupId = opts.getGroupId();
        this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
        this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
        this.writeLock.lock();
        try {
            if (this.defaultDB != null) {
                LOG.warn("LevelDBLogStorage init() already.");
                return true;
            }
            initAndLoad(opts.getConfigurationManager());
            return true;
        } catch (IOException | DBException e) {
            LOG.error("Fail to init LevelDBLogStorage, path={}.", this.homePath, e);
        } finally {
            this.writeLock.unlock();
        }
        return false;
    }

    private void openDatabase() throws DBException, IOException {
        if (this.opened) {
            return;
        }
        final File databaseHomeDir = new File(homePath);
        FileUtils.forceMkdir(databaseHomeDir);

        Options options = new Options();
        options.createIfMissing(true);

        File defaultDBFile = new File(databaseHomeDir, DEFAULT_DATABASE_NAME);
        File confDBFile = new File(databaseHomeDir, CONF_DATABASE_NAME);

        this.defaultDB = Iq80DBFactory.factory.open(defaultDBFile, options);
        this.confDB = Iq80DBFactory.factory.open(confDBFile, options);
        this.opened = true;
    }

    private void load(final ConfigurationManager confManager) {
        try (DBIterator iterator = this.confDB.iterator()) {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                final byte[] keyBytes = iterator.peekNext().getKey();
                final byte[] valueBytes = iterator.peekNext().getValue();
                if (keyBytes.length == Long.BYTES) {
                    final LogEntry entry = this.logEntryDecoder.decode(valueBytes);
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
                        LOG.warn("Fail to decode conf entry at index {}, the log data is: {}.",
                            Bits.getLong(keyBytes, 0), BytesUtil.toHex(valueBytes));
                    }
                } else if (Arrays.equals(FIRST_LOG_IDX_KEY, keyBytes)) {
                    // FIRST_LOG_IDX_KEY storage
                    setFirstLogIndex(Bits.getLong(valueBytes, 0));
                    truncatePrefixInBackground(0L, this.firstLogIndex);
                } else {
                    // Unknown entry
                    LOG.warn("Unknown entry in configuration storage key={}, value={}.", BytesUtil.toHex(keyBytes),
                        BytesUtil.toHex(valueBytes));
                }
            }
        } catch (IOException e) {
            LOG.error("Fail to load confDB.", e);
        }
    }

    private void initAndLoad(final ConfigurationManager confManager) throws DBException, IOException {
        this.hasLoadFirstLogIndex = false;
        this.firstLogIndex = 1;
        openDatabase();
        load(confManager);
    }

    private void closeDatabase() {
        this.opened = false;
        try {
            IOUtils.closeQuietly(this.defaultDB);
            IOUtils.closeQuietly(this.confDB);
        } catch (Exception e) {
            // ignore
        }
        this.defaultDB = null;
        this.confDB = null;
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            closeDatabase();
            LOG.info("LevelDBLogStorage shutdown, the db path is: {}.", this.homePath);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void describe(Printer out) {
        this.readLock.lock();
        try {
            if (opened) {
                out.println(String.format("Database is opened. the path: %s", this.homePath));
                out.println("LevelDB storage engine");
            } else {
                out.println(String.format("Database not open. the path: %s", this.homePath));
            }
        } finally {
            this.readLock.unlock();
        }
    }

    private void setFirstLogIndex(long firstLogIndex) {
        this.firstLogIndex = firstLogIndex;
        this.hasLoadFirstLogIndex = true;
    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex) {
                return this.firstLogIndex;
            }
            checkState();
            try (DBIterator iterator = this.defaultDB.iterator()) {
                iterator.seekToFirst();
                if (iterator.hasNext()) {
                    final byte[] keyBytes = iterator.peekNext().getKey();
                    final long firstLogIndex = Bits.getLong(keyBytes, 0);
                    saveFirstLogIndex(firstLogIndex);
                    setFirstLogIndex(firstLogIndex);
                    return firstLogIndex;
                }
            } catch (IOException e) {
                LOG.error("Fail to get first log index.", e);
            }
        } finally {
            this.readLock.unlock();
        }
        return 1L;
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        try {
            checkState();
            try (DBIterator iterator = this.defaultDB.iterator()) {
                iterator.seekToFirst();
                long lastIndex = 0;
                while (iterator.hasNext()) {
                    final byte[] keyBytes = iterator.peekNext().getKey();
                    lastIndex = Bits.getLong(keyBytes, 0);
                    iterator.next();
                }
                return lastIndex;
            } catch (IOException e) {
                LOG.error("Fail to get last log index.", e);
            }
        } finally {
            this.readLock.unlock();
        }
        return 0L;
    }

    @Override
    public LogEntry getEntry(long index) {
        this.readLock.lock();
        try {
            checkState();
            if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
                return null;
            }
            byte[] key = getKeyBytes(index);
            byte[] value = this.defaultDB.get(key);
            return toLogEntry(value);
        } catch (DBException e) {
            LOG.error("Fail to get log entry at index {}.", index, e);
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    @Override
    public long getTerm(long index) {
        final LogEntry entry = getEntry(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return 0;
    }

    @Override
    public boolean appendEntry(LogEntry entry) {
        if (entry == null) {
            return false;
        }
        this.readLock.lock();
        try {
            checkState();
            byte[] key = getKeyBytes(entry.getId().getIndex());
            byte[] value = toByteArray(entry);
            WriteBatch batch = this.defaultDB.createWriteBatch();
            try {
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    this.confDB.put(key, value);
                }
                batch.put(key, value);
                WriteOptions writeOpts = new WriteOptions();
                writeOpts.sync(this.sync);
                this.defaultDB.write(batch, writeOpts);
                return true;
            } finally {
                IOUtils.closeQuietly(batch);
            }
        } catch (DBException e) {
            LOG.error("Fail to append entry {}.", entry, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    @Override
    public int appendEntries(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        final int entriesCount = entries.size();
        this.readLock.lock();
        try {
            checkState();
            WriteBatch batch = this.defaultDB.createWriteBatch();
            try {
                for (int i = 0; i < entriesCount; i++) {
                    final LogEntry entry = entries.get(i);
                    byte[] key = getKeyBytes(entry.getId().getIndex());
                    byte[] value = toByteArray(entry);
                    if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                        this.confDB.put(key, value);
                    }
                    batch.put(key, value);
                }
                WriteOptions writeOpts = new WriteOptions();
                writeOpts.sync(this.sync);
                this.defaultDB.write(batch, writeOpts);
                return entriesCount;
            } finally {
                IOUtils.closeQuietly(batch);
            }
        } catch (DBException e) {
            LOG.error("Fail to appendEntries. first one = {}, entries count = {}", entries.get(0), entriesCount, e);
        } finally {
            this.readLock.unlock();
        }
        return 0;
    }

    @Override
    public boolean truncatePrefix(long firstIndexKept) {
        this.readLock.lock();
        try {
            checkState();
            final long startIndex = getFirstLogIndex();
            final boolean ret = saveFirstLogIndex(firstIndexKept);
            if (ret) {
                setFirstLogIndex(firstIndexKept);
            }
            truncatePrefixInBackground(startIndex, firstIndexKept);
            return true;
        } catch (DBException e) {
            LOG.error("Fail to truncatePrefix {}.", firstIndexKept, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept) {
        this.readLock.lock();
        try {
            checkState();
            final long lastLogIndex = getLastLogIndex();
            WriteBatch defaultBatch = this.defaultDB.createWriteBatch();
            try {
                for (long index = lastIndexKept + 1; index <= lastLogIndex; index++) {
                    byte[] key = getKeyBytes(index);
                    this.confDB.delete(key);
                    defaultBatch.delete(key);
                }
                WriteOptions writeOpts = new WriteOptions();
                writeOpts.sync(this.sync);
                this.defaultDB.write(defaultBatch, writeOpts);
                return true;
            } finally {
                IOUtils.closeQuietly(defaultBatch);
            }
        } catch (DBException e) {
            LOG.error("Fail to truncateSuffix {}.", lastIndexKept, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    @Override
    public boolean reset(long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.writeLock.lock();
        try {
            LogEntry entry = getEntry(nextLogIndex);
            closeDatabase();
            FileUtils.deleteDirectory(new File(this.homePath));
            initAndLoad(null);
            if (entry == null) {
                entry = new LogEntry();
                entry.setType(EntryType.ENTRY_TYPE_NO_OP);
                entry.setId(new LogId(nextLogIndex, 0));
                LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
            }
            return appendEntry(entry);
        } catch (IOException | DBException e) {
            LOG.error("Fail to reset next log index.", e);
        } finally {
            this.writeLock.unlock();
        }
        return false;
    }

    protected byte[] getKeyBytes(final long index) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, index);
        return ks;
    }

    protected boolean isSuccess(Object status) {
        return status == null || status.equals(Boolean.TRUE);
    }

    protected LogEntry toLogEntry(byte[] value) {
        if (value == null || value.length == 0) {
            return null;
        }
        return this.logEntryDecoder.decode(value);
    }

    protected byte[] toByteArray(LogEntry logEntry) {
        return this.logEntryEncoder.encode(logEntry);
    }

    /**
     * Save the first log index into confDB
     */
    private boolean saveFirstLogIndex(final long firstLogIndex) {
        this.readLock.lock();
        try {
            checkState();
            byte[] firstLogIndexValue = getKeyBytes(firstLogIndex);
            this.confDB.put(FIRST_LOG_IDX_KEY, firstLogIndexValue);
            return true;
        } catch (DBException e) {
            LOG.error("Fail to save first log index {}.", firstLogIndex, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    /**
     * [startIndex, firstIndexKept)
     */
    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept) {
        if (startIndex > firstIndexKept) {
            return;
        }
        // delete logs in background.
        final String groupId = this.groupId != null ? this.groupId : "leveldb_log_storage";
        ThreadPoolsFactory.runInThread(groupId, () -> {
            this.readLock.lock();
            try {
                checkState();
                WriteBatch batch = this.defaultDB.createWriteBatch();
                try {
                    for (long index = startIndex; index < firstIndexKept; index++) {
                        byte[] key = getKeyBytes(index);
                        this.confDB.delete(key); // Delete it first; otherwise, it may never be deleted
                        batch.delete(key);
                    }
                    WriteOptions writeOpts = new WriteOptions();
                    writeOpts.sync(this.sync);
                    this.defaultDB.write(batch, writeOpts);
                } finally {
                    IOUtils.closeQuietly(batch);
                }
            } catch (DBException e) {
                LOG.error("Fail to truncatePrefix {}.", firstIndexKept, e);
            } finally {
                this.readLock.unlock();
            }
        });
    }

    private void checkState() {
        Requires.requireTrue(opened, "Database not open. the path: %s", this.homePath);
    }
}