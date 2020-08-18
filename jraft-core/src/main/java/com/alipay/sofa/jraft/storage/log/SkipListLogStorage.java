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
package com.alipay.sofa.jraft.storage.log;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.util.*;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author: caobiao
 * @date: 2020/07/31
 * @Description:
 */
public class SkipListLogStorage implements LogStorage, Describer {
    private static final Logger                     LOG               = LoggerFactory
                                                                          .getLogger(RocksDBLogStorage.class);

    private final String                            path;

    private final ReadWriteLock                     readWriteLock     = new ReentrantReadWriteLock();
    private final Lock                              readLock          = this.readWriteLock.readLock();
    private final Lock                              writeLock         = this.readWriteLock.writeLock();

    protected ConcurrentSkipListMap<byte[], byte[]> datalogEntries;
    protected ConcurrentSkipListMap<byte[], byte[]> conflogEntries;

    protected LogEntryEncoder                       logEntryEncoder;
    protected LogEntryDecoder                       logEntryDecoder;

    private volatile long                           firstLogIndex     = 1;
    private volatile boolean                        hasLoadFirstLogIndex;
    private final boolean                           sync;

    /**
     * First log index and last log index key in configuration column family.
     */
    public static final byte[]                      FIRST_LOG_IDX_KEY = Utils.getBytes("meta/firstLogIndex");

    public SkipListLogStorage(final String path, final RaftOptions raftOptions) {
        super();
        this.path = path;
        this.sync = raftOptions.isSync();
    }

    protected void addConfPure(byte[] keyBytes, byte[] metadata) {
        this.conflogEntries.put(keyBytes, metadata);
    }

    protected void addDataPure(byte[] keyBytes, byte[] metadata) {
        this.datalogEntries.put(keyBytes, metadata);
    }

    protected void putFirstKeyBytes(byte[] firstKey) {
        this.conflogEntries.put(FIRST_LOG_IDX_KEY, firstKey);
    }

    protected static class EmptyWriteContext implements RocksDBLogStorage.WriteContext {
        static ArrayDequeLogStorage.EmptyWriteContext INSTANCE = new ArrayDequeLogStorage.EmptyWriteContext();
    }

    private boolean initAndLoad(final ConfigurationManager confManager) {
        this.hasLoadFirstLogIndex = false;
        this.firstLogIndex = 1;
        initMap();
        boolean b = onInitLoaded();
        if (b) {
            load(confManager);
        }
        return b;
    }

    private void initMap() {
        this.datalogEntries = new ConcurrentSkipListMap<>((o1, o2) -> {
            long res = Bits.getLong(o1, 0) - Bits.getLong(o2, 0);
            return res > 0 ? 1 : (res == 0 ? 0 : -1);
        });
        this.conflogEntries = new ConcurrentSkipListMap<>((o1, o2) -> {
            long res = Bits.getLong(o1, 0) - Bits.getLong(o2, 0);
            return res > 0 ? 1 : (res == 0 ? 0 : -1);
        });
    }

    private void load(ConfigurationManager confManager) {
        checkState();
        for (Map.Entry<byte[], byte[]> e : conflogEntries.entrySet()) {
            final byte[] ks = e.getKey();
            final byte[] bs = e.getValue();
            // LogEntry index
            if (ks.length == 8) {
                final LogEntry entry = getEntry(Bits.getLong(ks, 0));
                if (entry != null) {
                    if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
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
                    deletePrefixRange(this.conflogEntries, this.firstLogIndex);
                } else {
                    LOG.warn("Unknown entry in configuration storage key={}, value={}.", BytesUtil.toHex(ks),
                        BytesUtil.toHex(bs));
                }
            }
        }
    }

    private void checkState() {
        Requires.requireNonNull(this.datalogEntries, "datalogEntries not initialized or destroyed");
        Requires.requireNonNull(this.conflogEntries, "conflogEntries not initialized or destroyed");
    }

    protected static byte[] getKeyBytes(final long index) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, index);
        return ks;
    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex) {
                return this.firstLogIndex;
            }
            checkState();
            if (this.datalogEntries.size() > 0) {
                final long ret = Bits.getLong(this.datalogEntries.firstKey(), 0);
                saveFirstLogIndex(ret);
                setFirstLogIndex(ret);
                return ret;
            }
            return 1L;
        } finally {
            this.readLock.unlock();
        }
    }

    private void setFirstLogIndex(long ret) {
        this.firstLogIndex = ret;
        this.hasLoadFirstLogIndex = true;
    }

    private boolean saveFirstLogIndex(long ret) {
        this.readLock.lock();
        try {
            final byte[] vs = new byte[8];
            Bits.putLong(vs, 0, ret);
            checkState();
            this.conflogEntries.put(FIRST_LOG_IDX_KEY, onFirstLogIndexAppend(vs));
            return true;
        } finally {
            this.readLock.unlock();
        }
    }

    protected void deletePrefixRange(ConcurrentSkipListMap<byte[], byte[]> map, long firstIndexKept) {
        while (!map.isEmpty()) {
            byte[] firstKey = map.firstKey();
            if (firstKey.length != 8 && map.size() == 1) {
                break;
            }
            if (firstKey.length != 8) {
                continue;
            }
            if (Bits.getLong(firstKey, 0) < firstIndexKept) {
                map.pollFirstEntry();
            } else {
                return;
            }
        }
    }

    protected void deleteSuffixRange(ConcurrentSkipListMap<byte[], byte[]> map, long lastIndexKept) {
        while (!map.isEmpty()) {
            byte[] lastKey = map.lastKey();
            if (lastKey.length != 8 && map.size() == 1) {
                break;
            }
            if (lastKey.length != 8) {
                continue;
            }
            if (Bits.getLong(lastKey, 0) > lastIndexKept) {
                map.pollLastEntry();
            } else {
                return;
            }
        }
    }

    protected byte[] onFirstLogIndexAppend(byte[] vs) {
        return vs;
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        checkState();
        try {
            if (datalogEntries.size() > 0) {
                return Bits.getLong(datalogEntries.lastKey(), 0);
            }
            return 0L;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(long index) {
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
                return null;
            }
            final byte[] bs = onDataGet(index, getValueFromSkipList(index));
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
        } catch (final IOException e) {
            LOG.error("Fail to get log entry at index {}.", index, e);
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    protected byte[] getValueFromSkipList(long index) {
        checkState();
        return datalogEntries.get(getKeyBytes(index));
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
        final RocksDBLogStorage.WriteContext writeCtx = newWriteContext();
        if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
            try {
                return addConf(entry, writeCtx);
            } catch (IOException e) {
                LOG.error("Fail to append entry.", e);
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        } else {
            this.readLock.lock();
            try {
                if (this.datalogEntries == null) {
                    LOG.warn("Deque not initialized or destroyed.");
                    return false;
                }
                final long logIndex = entry.getId().getIndex();
                final byte[] valueBytes = this.logEntryEncoder.encode(entry);
                final byte[] newValueBytes = onDataAppend(logIndex, valueBytes, writeCtx);
                writeCtx.startJob();
                this.datalogEntries.put(getKeyBytes(logIndex), newValueBytes);
                writeCtx.joinAll();
                if (newValueBytes != valueBytes) {
                    doSync();
                }
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (IOException e) {
                LOG.error("Fail to append entry.", e);
                return false;
            } finally {
                this.readLock.unlock();
            }
        }
    }

    @Override
    public int appendEntries(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        final int entriesCount = entries.size();
        final RocksDBLogStorage.WriteContext writeCtx = newWriteContext();
        try {
            for (final LogEntry entry : entries) {
                if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                    writeCtx.startJob();
                    addConf(entry, writeCtx);
                } else {
                    writeCtx.startJob();
                    addData(entry, writeCtx);
                }
            }
            writeCtx.joinAll();
            doSync();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return entriesCount;
    }

    @Override
    public boolean truncatePrefix(long firstIndexKept) {
        final long startIndex = getFirstLogIndex();
        final boolean ret = saveFirstLogIndex(firstIndexKept);
        if (ret) {
            setFirstLogIndex(firstIndexKept);
        }
        this.readLock.lock();
        try {
            if (!this.datalogEntries.isEmpty() && !this.conflogEntries.isEmpty()) {
                onTruncatePrefix(startIndex, firstIndexKept);
                deletePrefixRange(datalogEntries, firstIndexKept);
                deletePrefixRange(conflogEntries, firstIndexKept);
            }
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to truncatePrefix {}.", firstIndexKept, e);
        } finally {
            this.readLock.unlock();
        }
        return ret;
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept) {
        this.readLock.lock();
        try {
            try {
                onTruncateSuffix(lastIndexKept);
            } finally {
                deleteSuffixRange(datalogEntries, lastIndexKept);
                deleteSuffixRange(conflogEntries, lastIndexKept);
            }
            return true;
        } catch (final RocksDBException | IOException e) {
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
            this.conflogEntries.clear();
            this.datalogEntries.clear();
            this.conflogEntries = null;
            this.datalogEntries = null;
            onReset(nextLogIndex);
            if (initAndLoad(null)) {
                if (entry == null) {
                    entry = new LogEntry();
                    entry.setType(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
                    entry.setId(new LogId(nextLogIndex, 0));
                    LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
                }
                return appendEntry(entry);
            } else {
                return false;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean init(LogStorageOptions opts) {
        Requires.requireNonNull(opts.getConfigurationManager(), "Null conf manager");
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        this.writeLock.lock();
        try {
            if (this.datalogEntries != null || this.conflogEntries != null) {
                LOG.warn("init() already.");
                return true;
            }
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            Requires.requireNonNull(this.logEntryDecoder, "Null log entry decoder");
            Requires.requireNonNull(this.logEntryEncoder, "Null log entry encoder");
            return initAndLoad(opts.getConfigurationManager());
        } catch (final Exception e) {
            LOG.error("Fail to init.", e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            onShutdown();
            this.conflogEntries.clear();
            this.datalogEntries.clear();
            this.conflogEntries = null;
            this.datalogEntries = null;
            LOG.info("SkipList destroyed.");
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void describe(Printer out) {

    }

    private void doSync() throws IOException, InterruptedException {
        onSync();
    }

    private void addData(final LogEntry entry, final RocksDBLogStorage.WriteContext ctx) throws IOException,
                                                                                        InterruptedException {
        final long logIndex = entry.getId().getIndex();
        final byte[] content = this.logEntryEncoder.encode(entry);
        final byte[] newValueBytes = onDataAppend(logIndex, content, ctx);
        this.datalogEntries.put(getKeyBytes(logIndex), newValueBytes);
    }

    private boolean addConf(final LogEntry entry, final RocksDBLogStorage.WriteContext ctx) throws IOException,
                                                                                           InterruptedException {
        final long logIndex = entry.getId().getIndex();
        final byte[] content = this.logEntryEncoder.encode(entry);
        final byte[] newValueBytes = onDataAppend(logIndex, content, ctx);
        this.conflogEntries.put(getKeyBytes(logIndex), newValueBytes);
        return true;
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
        System.out.println("onTruncatePrefix");
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

    protected RocksDBLogStorage.WriteContext newWriteContext() {
        return ArrayDequeLogStorage.EmptyWriteContext.INSTANCE;
    }

    /**
     * Called before appending data entry.
     *
     * @param logIndex the log index
     * @param value    the data value in log entry.
     * @return the new value
     */
    protected byte[] onDataAppend(final long logIndex, final byte[] value, final RocksDBLogStorage.WriteContext ctx)
                                                                                                                    throws IOException,
                                                                                                                    InterruptedException {
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
}
