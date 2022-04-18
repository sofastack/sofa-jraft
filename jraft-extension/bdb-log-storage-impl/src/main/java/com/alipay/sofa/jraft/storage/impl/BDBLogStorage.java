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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;

/**
 * Log storage based on bdb.
 * 
 * @author cff
 *
 */
public class BDBLogStorage implements LogStorage, Describer {

    private static final Logger LOG                   = LoggerFactory.getLogger(BDBLogStorage.class);
    static final String         DEFAULT_DATABASE_NAME = "jraft-log";
    static final String         CONF_DATABASE_NAME    = "jraft-conf";

    private Database            defaultTable;
    private Database            confTable;
    private Environment         environment;
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

    public BDBLogStorage(final String homePath, final RaftOptions raftOptions) {
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
        this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
        this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
        this.writeLock.lock();
        try {
            if (this.defaultTable != null) {
                LOG.warn("BDBLogStorage init() already.");
                return true;
            }
            initAndLoad(opts.getConfigurationManager());
            return true;
        } catch (IOException | DatabaseException e) {
            LOG.error("Fail to init BDBLogStorage, path={}.", this.homePath, e);
        } finally {
            this.writeLock.unlock();
        }
        return false;
    }

    private void openDatabase() throws DatabaseException, IOException {
        if (this.opened) {
            return;
        }
        final File databaseHomeDir = new File(homePath);
        FileUtils.forceMkdir(databaseHomeDir);
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setTransactional(true);
        environmentConfig.setAllowCreate(true);
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(true);
        this.environment = new Environment(databaseHomeDir, environmentConfig);
        this.defaultTable = this.environment.openDatabase(null, DEFAULT_DATABASE_NAME, databaseConfig);
        this.confTable = this.environment.openDatabase(null, CONF_DATABASE_NAME, databaseConfig);
        this.opened = true;
    }

    private void load(final ConfigurationManager confManager) {
        try (Cursor cursor = this.confTable.openCursor(null, new CursorConfig())) {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus operationStatus = cursor.getFirst(key, data, LockMode.DEFAULT);
            while (isSuccessOperation(operationStatus)) {
                final byte[] keyBytes = key.getData();
                final byte[] valueBytes = data.getData();
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
                operationStatus = cursor.getNext(key, data, LockMode.DEFAULT);
            }
        }
    }

    private void initAndLoad(final ConfigurationManager confManager) throws DatabaseException, IOException {
        this.hasLoadFirstLogIndex = false;
        this.firstLogIndex = 1;
        openDatabase();
        load(confManager);
    }

    private void closeDatabase() {
        this.opened = false;
        try {
            IOUtils.close(defaultTable, confTable, environment);
        } catch (IOException e) {
            // ignore
        }
        this.defaultTable = null;
        this.confTable = null;
        this.environment = null;

    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            closeDatabase();
            LOG.info("BDBLogStorage shutdown, the db path is: {}.", this.homePath);
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
                StatsConfig statsConfig = new StatsConfig();
                EnvironmentStats stats = environment.getStats(statsConfig);
                out.println(stats);
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
            try (Cursor cursor = this.defaultTable.openCursor(null, new CursorConfig());) {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                OperationStatus operationStatus = cursor.getFirst(key, data, LockMode.DEFAULT);
                if (OperationStatus.SUCCESS.equals(operationStatus)) {
                    final long firstLogIndex = Bits.getLong(key.getData(), 0);
                    saveFirstLogIndex(firstLogIndex);
                    setFirstLogIndex(firstLogIndex);
                    return firstLogIndex;
                }
            }
        } finally {
            this.readLock.unlock();
        }
        return 1L;
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        Cursor cursor = null;
        try {
            checkState();
            cursor = this.defaultTable.openCursor(null, new CursorConfig());
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus operationStatus = cursor.getLast(key, data, LockMode.DEFAULT);
            if (OperationStatus.SUCCESS.equals(operationStatus)) {
                return Bits.getLong(key.getData(), 0);
            }
        } finally {
            this.readLock.unlock();
            if (cursor != null) {
                cursor.close();
            }
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
            DatabaseEntry logEntry = new DatabaseEntry();
            OperationStatus operationStatus = this.defaultTable.get(null, getKeyDatabaseEntry(index), logEntry,
                LockMode.DEFAULT);
            if (isSuccessOperation(operationStatus)) {
                return toLogEntry(logEntry);
            }
        } catch (DatabaseException e) {
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
        Transaction txn = null;
        try {
            checkState();
            DatabaseEntry key = getKeyDatabaseEntry(entry.getId().getIndex());
            DatabaseEntry value = toDatabaseEntry(entry);
            txn = beginTransaction();
            if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                this.confTable.put(txn, key, value);
            }
            this.defaultTable.put(txn, key, value);
            txn.commit();
            syncIfNeed();
            return true;
        } catch (DatabaseException e) {
            LOG.error("Fail to append entry {}.", entry, e);
            if (txn != null) {
                txn.abort();
            }
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
        Transaction txn = null;
        try {
            checkState();
            txn = beginTransaction();
            for (int i = 0; i < entriesCount; i++) {
                final LogEntry entry = entries.get(i);
                DatabaseEntry key = getKeyDatabaseEntry(entry.getId().getIndex());
                DatabaseEntry value = toDatabaseEntry(entry);
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    this.confTable.put(txn, key, value);
                }
                this.defaultTable.put(txn, key, value);
            }
            txn.commit();
            syncIfNeed();
            return entriesCount;
        } catch (DatabaseException e) {
            LOG.error("Fail to appendEntries. first one = {}, entries count = {}", entries.get(0), entriesCount, e);
            if (txn != null) {
                txn.abort();
            }
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
        } catch (DatabaseException e) {
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
            for (long index = lastIndexKept + 1; index <= lastLogIndex; index++) {
                DatabaseEntry key = getKeyDatabaseEntry(index);
                this.confTable.delete(null, key);
                this.defaultTable.delete(null, key);
            }
            return true;
        } catch (DatabaseException e) {
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
        } catch (IOException | DatabaseException e) {
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

    protected DatabaseEntry getKeyDatabaseEntry(final long index) {
        return new DatabaseEntry(getKeyBytes(index));
    }

    protected boolean isSuccessOperation(OperationStatus status) {
        return status != null && OperationStatus.SUCCESS.equals(status);
    }

    protected LogEntry toLogEntry(DatabaseEntry logEntry) {
        if (logEntry == null || logEntry.getSize() == 0) {
            return null;
        }
        return this.logEntryDecoder.decode(logEntry.getData());
    }

    protected DatabaseEntry toDatabaseEntry(LogEntry logEntry) {
        return new DatabaseEntry(this.logEntryEncoder.encode(logEntry));
    }

    private void syncIfNeed() {
        if (sync) {
            this.environment.sync();
        }
    }

    /**
     * Save the first log index into {@link BDBLogStorage#confTable} }
     */
    private boolean saveFirstLogIndex(final long firstLogIndex) {
        this.readLock.lock();
        try {
            checkState();
            DatabaseEntry firstLogIndexKey = new DatabaseEntry(FIRST_LOG_IDX_KEY);
            DatabaseEntry firstLogIndexValue = getKeyDatabaseEntry(firstLogIndex);
            this.confTable.put(null, firstLogIndexKey, firstLogIndexValue);
            syncIfNeed();
            return true;
        } catch (DatabaseException e) {
            LOG.error("Fail to save first log index {}.", firstLogIndex, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    private Transaction beginTransaction() {
        return environment.beginTransaction(null, null);
    }

    /**
     * [startIndex, firstIndexKept)
     */
    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept) {
		if (startIndex > firstIndexKept) {
			return;
		}
		// delete logs in background.
		Utils.runInThread(() -> {
			this.readLock.lock();
			try {
				checkState();
				for (long index = startIndex; index < firstIndexKept; index++) {
					DatabaseEntry key = getKeyDatabaseEntry(index);
					this.confTable.delete(null, key);// Delete it first; otherwise, it may never be deleted
					this.defaultTable.delete(null, key);
				}
			} catch (DatabaseException e) {
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
