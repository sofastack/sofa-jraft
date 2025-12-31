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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.ThreadPoolsFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Log storage based on MapDB.
 *
 * @author knightblood
 */
public class MapDBLogStorage implements LogStorage, Describer {

    private static final Logger      LOG               = LoggerFactory.getLogger(MapDBLogStorage.class);
    static final String              DEFAULT_MAP_NAME  = "jraft-log";
    static final String              CONF_MAP_NAME     = "jraft-conf";

    private String                   groupId;
    private HTreeMap<byte[], byte[]> defaultMap;
    private HTreeMap<byte[], byte[]> confMap;
    private DB                       db;
    private final String             homePath;
    private boolean                  opened            = false;
    private final boolean            sync;

    private LogEntryEncoder          logEntryEncoder;
    private LogEntryDecoder          logEntryDecoder;

    private final ReadWriteLock      readWriteLock     = new ReentrantReadWriteLock();
    private final Lock               readLock          = this.readWriteLock.readLock();
    private final Lock               writeLock         = this.readWriteLock.writeLock();

    private volatile long            firstLogIndex     = 1;
    private volatile boolean         hasLoadFirstLogIndex;

    private ScheduledExecutorService flushExecutorService;
    private ScheduledFuture<?>       flushScheduledFuture;
    private volatile boolean         needFlush         = false;
    private final int                flushIntervalMs   = 100;                                           // 定期flush间隔（毫秒）

    // 添加批量写入相关字段
    private final int                batchSize         = 100;                                           // 批量提交大小
    private final List<LogEntry>     writeBuffer       = new ArrayList<>();                             // 写缓冲区
    private final Object             bufferLock        = new Object();                                  // 缓冲区锁

    /**
     * First log index and last log index key in configuration column family.
     */
    public static final byte[]       FIRST_LOG_IDX_KEY = Utils.getBytes("meta/firstLogIndex");

    /**
     * 检查当前 JVM 是否为 64 位。
     */
    private boolean is64BitJVM() {
        String model = System.getProperty("sun.arch.data.model");
        if (model != null && model.equals("64")) {
            return true;
        }
        return false;
    }

    public MapDBLogStorage(final String homePath, final RaftOptions raftOptions) {
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
            if (this.db != null) {
                LOG.warn("MapDBLogStorage init() already.");
                return true;
            }
            initAndLoad(opts.getConfigurationManager());
            return true;
        } catch (Exception e) {
            LOG.error("Fail to init MapDBLogStorage, path={}.", this.homePath, e);
        } finally {
            this.writeLock.unlock();
        }
        return false;
    }

    private void openDatabase() throws Exception {
        if (this.opened) {
            return;
        }
        final File databaseHomeDir = new File(homePath);
        FileUtils.forceMkdir(databaseHomeDir);

        File dbFile = new File(databaseHomeDir, "mapdb-log.db");
        DBMaker.Maker maker = DBMaker.fileDB(dbFile);
        
        // 启用性能优化选项
        if (!this.sync) {
            // 非同步模式下启用JVM关闭时自动关闭
            maker = maker.closeOnJvmShutdown();
        } else {
            // 同步模式下启用事务支持
            maker = maker.transactionEnable();
        }
        
        // 启用内存映射文件以提高性能（仅在64位系统上）
        maker = maker.fileMmapEnable().fileMmapEnableIfSupported().fileMmapPreclearDisable();
        
        // 启用文件通道以提高性能
        maker = maker.fileChannelEnable();
        
        // 在64位JVM上启用cleaner hack以提高性能
        maker = maker.cleanerHackEnable();
        
        this.db = maker.make();
        this.defaultMap = this.db.hashMap(DEFAULT_MAP_NAME, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
            .createOrOpen();
        this.confMap = this.db.hashMap(CONF_MAP_NAME, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen();
        this.opened = true;
        
        // 初始化flush executor并启动定期flush任务
        if (!this.sync) {
            this.flushExecutorService = ThreadPoolUtil.newScheduledBuilder()
                .poolName("mapdb-flush-executor")
                .enableMetric(true)
                .coreThreads(1)
                .threadFactory(new NamedThreadFactory("MapDB-Flush-Thread-", true))
                .build();
            this.flushScheduledFuture = this.flushExecutorService.scheduleWithFixedDelay(this::flushDatabase, 
                flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Flush数据库以确保数据持久化
     */
    private void flushDatabase() {
        // 在非同步模式下定期flush数据
        if (!this.sync && needFlush) {
            this.writeLock.lock();
            try {
                synchronized (bufferLock) {
                    if (needFlush) {
                        // 提交缓冲区中的数据
                        if (!writeBuffer.isEmpty()) {
                            commitBuffer();
                        }
                        // 提交数据库
                        this.db.commit();
                        needFlush = false;
                    }
                }
            } catch (Exception e) {
                LOG.error("Fail to flush mapdb.", e);
            } finally {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * 提交缓冲区中的数据到MapDB
     */
    private void commitBuffer() {
        if (writeBuffer.isEmpty()) {
            return;
        }

        synchronized (bufferLock) {
            if (writeBuffer.isEmpty()) {
                return;
            }

            // 批量处理缓冲区中的日志条目
            for (LogEntry entry : writeBuffer) {
                byte[] key = getKeyBytes(entry.getId().getIndex());
                byte[] value = toByteArray(entry);
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    this.confMap.put(key, value);
                }
                this.defaultMap.put(key, value);
            }

            writeBuffer.clear();
        }
    }

    private void load(final ConfigurationManager confManager) {
        try {
            for (byte[] keyBytes : this.confMap.getKeys()) {
                final byte[] valueBytes = this.confMap.get(keyBytes);
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
        } catch (Exception e) {
            LOG.error("Fail to load confMap.", e);
        }
    }

    private void initAndLoad(final ConfigurationManager confManager) throws Exception {
        this.hasLoadFirstLogIndex = false;
        this.firstLogIndex = 1;
        openDatabase();
        load(confManager);
    }

    private void closeDatabase() {
        this.opened = false;

        // 提交缓冲区中的剩余数据
        if (!this.sync && !writeBuffer.isEmpty()) {
            try {
                commitBuffer();
                if (this.db != null) {
                    this.db.commit();
                }
            } catch (Exception e) {
                LOG.error("Fail to commit remaining buffer data.", e);
            }
        }

        // 关闭定期flush任务
        if (this.flushScheduledFuture != null) {
            this.flushScheduledFuture.cancel(true);
        }

        if (this.flushExecutorService != null) {
            this.flushExecutorService.shutdown();
            try {
                if (!this.flushExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    this.flushExecutorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                this.flushExecutorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        try {
            if (this.db != null) {
                this.db.commit();
                this.db.close();
            }
        } catch (Exception e) {
            LOG.error("Fail to close mapdb.", e);
        }
        this.db = null;
        this.defaultMap = null;
        this.confMap = null;

        // 强制垃圾回收以释放可能被占用的文件句柄
        System.gc();
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            closeDatabase();
            LOG.info("MapDBLogStorage shutdown, the db path is: {}.", this.homePath);
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
                out.println("MapDB storage engine");
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
        if (this.hasLoadFirstLogIndex) {
            return this.firstLogIndex;
        }
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex) {
                return this.firstLogIndex;
            }
            checkState();
            try {
                Iterator<byte[]> iterator = this.defaultMap.getKeys().iterator();
                byte[] firstKey = null;
                long minIndex = Long.MAX_VALUE;
                boolean hasKey = false;

                // 遍历所有键，找到最小的索引值
                while (iterator.hasNext()) {
                    hasKey = true;
                    byte[] key = iterator.next();
                    if (key != null && key.length == Long.BYTES) {
                        long index = Bits.getLong(key, 0);
                        if (index < minIndex) {
                            minIndex = index;
                            firstKey = key;
                        }
                    }
                }

                if (hasKey && firstKey != null) {
                    final long firstLogIndex = minIndex;
                    saveFirstLogIndex(firstLogIndex);
                    setFirstLogIndex(firstLogIndex);
                    return firstLogIndex;
                }
            } catch (Exception e) {
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
            try {
                Iterator<byte[]> iterator = this.defaultMap.getKeys().iterator();
                byte[] lastKey = null;
                long maxIndex = Long.MIN_VALUE;
                boolean hasKey = false;

                // 遍历所有键，找到最大的索引值
                while (iterator.hasNext()) {
                    hasKey = true;
                    byte[] key = iterator.next();
                    if (key != null && key.length == Long.BYTES) {
                        long index = Bits.getLong(key, 0);
                        if (index > maxIndex) {
                            maxIndex = index;
                            lastKey = key;
                        }
                    }
                }

                if (hasKey && lastKey != null) {
                    return maxIndex;
                }
            } catch (Exception e) {
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
            byte[] value = this.defaultMap.get(key);
            return toLogEntry(value);
        } catch (Exception e) {
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

            // 如果是同步模式直接写入并提交
            if (this.sync) {
                // 直接写入单个条目
                return writeEntryDirectly(entry);
            }
            // 非同步模式下使用缓冲区
            else {
                // 将单个条目添加到缓冲区
                return bufferEntry(entry);
            }
        } catch (Exception e) {
            LOG.error("Fail to append entry {}.", entry, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    /**
     * 直接写入单个日志条目并立即提交
     */
    private boolean writeEntryDirectly(LogEntry entry) {
        byte[] key = getKeyBytes(entry.getId().getIndex());
        byte[] value = toByteArray(entry);
        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
            this.confMap.put(key, value);
        }
        this.defaultMap.put(key, value);

        this.db.commit(); // 同步提交
        return true;
    }

    /**
     * 将单个日志条目添加到缓冲区
     */
    private boolean bufferEntry(LogEntry entry) {
        synchronized (bufferLock) {
            writeBuffer.add(entry);

            // 如果达到批处理大小，立即提交
            if (writeBuffer.size() >= batchSize) {
                commitBuffer();
                this.db.commit();
                return true;
            }
        }

        // 标记需要定期刷新
        needFlush = true;
        return true;
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

            // 如果是同步模式直接写入并提交
            if (this.sync) {
                return writeEntriesDirectly(entries);
            }
            // 非同步模式下使用缓冲区
            else {
                return bufferEntries(entries);
            }
        } catch (Exception e) {
            LOG.error("Fail to appendEntries. first one = {}, entries count = {}", entries.get(0), entriesCount, e);
        } finally {
            this.readLock.unlock();
        }
        return 0;
    }

    /**
     * 直接写入日志条目列表并立即提交
     */
    private int writeEntriesDirectly(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            byte[] key = getKeyBytes(entry.getId().getIndex());
            byte[] value = toByteArray(entry);
            if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                this.confMap.put(key, value);
            }
            this.defaultMap.put(key, value);
        }

        this.db.commit(); // 同步提交
        return entries.size();
    }

    /**
     * 将日志条目列表添加到缓冲区
     */
    private int bufferEntries(List<LogEntry> entries) {
        synchronized (writeBuffer) {
            int totalAdded = 0;
            int bufferSize = writeBuffer.size();
            for (LogEntry entry : entries) {
                writeBuffer.add(entry);
                totalAdded++;
                bufferSize++;

                // 如果达到批处理大小，立即提交
                if (bufferSize >= batchSize) {
                    commitBuffer();
                    bufferSize = 0;
                }
            }

            // 最后提交一次剩余的日志
            if (bufferSize > 0) {
                commitBuffer();
                this.db.commit();
            } else {
                this.db.commit();
            }

            return totalAdded;
        }
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
        } catch (Exception e) {
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
                byte[] key = getKeyBytes(index);
                // Delete it first; otherwise, it may never be deleted
                this.confMap.remove(key); // Delete it first; otherwise, it may never be deleted
                this.defaultMap.remove(key);
            }
            this.db.commit();
            return true;
        } catch (Exception e) {
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
        } catch (Exception e) {
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
     * Save the first log index into confMap
     */
    private boolean saveFirstLogIndex(final long firstLogIndex) {
        this.readLock.lock();
        try {
            checkState();
            byte[] firstLogIndexValue = getKeyBytes(firstLogIndex);
            this.confMap.put(FIRST_LOG_IDX_KEY, firstLogIndexValue);
            this.db.commit();
            return true;
        } catch (Exception e) {
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
        ThreadPoolsFactory.runInThread(this.groupId, () -> {
            this.writeLock.lock();
            try {
                checkState();
                for (long index = startIndex; index < firstIndexKept; index++) {
                    byte[] key = getKeyBytes(index);
                    this.confMap.remove(key);
                    this.defaultMap.remove(key);
                }
                this.db.commit();
            } catch (Exception e) {
                LOG.error("Fail to truncatePrefix {}.", firstIndexKept, e);
            } finally {
                this.writeLock.unlock();
            }
        });
    }

    private void checkState() {
        Requires.requireTrue(opened, "Database not open. the path: %s", this.homePath);
    }
}