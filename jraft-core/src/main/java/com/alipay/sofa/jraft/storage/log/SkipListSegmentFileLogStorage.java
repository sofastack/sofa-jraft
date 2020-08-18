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

import com.alipay.sofa.common.profile.StringUtil;
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
import com.alipay.sofa.jraft.storage.log.CheckpointFile.Checkpoint;
import com.alipay.sofa.jraft.storage.log.SegmentFile.SegmentFileOptions;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.CountDownEvent;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Platform;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.Utils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

/**
 * @author: caobiao
 * @date: 2020/07/31
 * @Description: Log Storage implementation based on rocksdb and segment files.
 */
public class SkipListSegmentFileLogStorage implements LogStorage {

    protected ConcurrentSkipListMap<byte[], byte[]> datalogEntries;
    protected ConcurrentSkipListMap<byte[], byte[]> conflogEntries;

    protected LogEntryEncoder                       logEntryEncoder;
    protected LogEntryDecoder                       logEntryDecoder;
    private volatile long                           firstLogIndex              = 1;
    private volatile boolean                        hasLoadFirstLogIndex;
    private final boolean                           sync;

    private static final int                        PRE_ALLOCATE_SEGMENT_COUNT = 2;
    private static final int                        MEM_SEGMENT_COUNT          = 3;
    private final ReadWriteLock                     readWriteLock              = new ReentrantReadWriteLock();
    private final Lock                              writeLock                  = this.readWriteLock.writeLock();
    private final Lock                              readLock                   = this.readWriteLock.readLock();

    public static final byte[]                      FIRST_LOG_IDX_KEY          = Utils.getBytes("meta/firstLogIndex");

    public SkipListSegmentFileLogStorage(final String path, final RaftOptions raftOptions) {
        this(path, raftOptions, MAX_SEGMENT_FILE_SIZE);
    }

    public SkipListSegmentFileLogStorage(final String path, final RaftOptions raftOptions, final int maxSegmentFileSize) {
        this(path, raftOptions, maxSegmentFileSize, PRE_ALLOCATE_SEGMENT_COUNT, MEM_SEGMENT_COUNT,
            DEFAULT_CHECKPOINT_INTERVAL_MS, createDefaultWriteExecutor());
    }

    private void addConfPure(byte[] keyBytes, byte[] metadata) {
        this.conflogEntries.put(keyBytes, metadata);
    }

    private void addDataPure(byte[] keyBytes, byte[] metadata) {
        this.datalogEntries.put(keyBytes, metadata);
    }

    private void putFirstKeyBytes(byte[] firstKey) {
        this.conflogEntries.put(FIRST_LOG_IDX_KEY, firstKey);
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
        } catch (final IOException e) {
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

    private void checkState() {
        Requires.requireNonNull(this.datalogEntries, "datalogEntries not initialized or destroyed");
        Requires.requireNonNull(this.conflogEntries, "conflogEntries not initialized or destroyed");
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

    protected byte[] getValueFromSkipList(long index) {
        checkState();
        return datalogEntries.get(getKeyBytes(index));
    }

    protected static byte[] getKeyBytes(final long index) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, index);
        return ks;
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

    private static class AllocatedResult {
        SegmentFile segmentFile;
        IOException ie;

        public AllocatedResult(final SegmentFile segmentFile) {
            super();
            this.segmentFile = segmentFile;
        }

        public AllocatedResult(final IOException ie) {
            super();
            this.ie = ie;
        }

    }

    public static class BarrierWriteContext implements RocksDBLogStorage.WriteContext {
        private final CountDownEvent    events = new CountDownEvent();
        private volatile Exception      e;
        private volatile List<Runnable> hooks;

        @Override
        public void startJob() {
            this.events.incrementAndGet();
        }

        @Override
        public synchronized void addFinishHook(final Runnable r) {
            if (this.hooks == null) {
                this.hooks = new CopyOnWriteArrayList<>();
            }
            this.hooks.add(r);
        }

        @Override
        public void finishJob() {
            this.events.countDown();
        }

        @Override
        public void setError(final Exception e) {
            this.e = e;
        }

        @Override
        public void joinAll() throws InterruptedException, IOException {
            this.events.await();
            if (this.hooks != null) {
                for (Runnable r : this.hooks) {
                    r.run();
                }
            }
            if (this.e != null) {
                throw new IOException("Fail to apppend entries", this.e);
            }
        }

    }

    private static final String SEGMENT_FILE_POSFIX            = ".s";

    private static final Logger LOG                            = LoggerFactory
                                                                   .getLogger(SkipListSegmentFileLogStorage.class);

    /**
     * Default checkpoint interval in milliseconds.
     */
    private static final int    DEFAULT_CHECKPOINT_INTERVAL_MS = SystemPropertyUtil.getInt(
                                                                   "jraft.log_storage.segment.checkpoint.interval.ms",
                                                                   5000);

    /**
     * Location metadata format:
     * 1. magic bytes
     * 2. reserved(2 B)
     * 3. segmentFileName(8 B)
     * 4. wrotePosition(4 B)
     */
    private static final int    LOCATION_METADATA_SIZE         = SegmentFile.RECORD_MAGIC_BYTES_SIZE + 2 + 8 + 4;

    /**
     * Max segment file size, 1G
     */
    private static final int    MAX_SEGMENT_FILE_SIZE          = SystemPropertyUtil.getInt(
                                                                   "jraft.log_storage.segment.max.size.bytes",
                                                                   1024 * 1024 * 1024);

    /**
     * RocksDBSegmentLogStorage builder
     * @author boyan(boyan@antfin.com)
     *
     */
    public static class Builder {
        private String             path;
        private RaftOptions        raftOptions;
        private int                maxSegmentFileSize       = MAX_SEGMENT_FILE_SIZE;
        private ThreadPoolExecutor writeExecutor;
        private int                preAllocateSegmentCount  = PRE_ALLOCATE_SEGMENT_COUNT;
        private int                keepInMemorySegmentCount = MEM_SEGMENT_COUNT;
        private int                checkpointIntervalMs     = DEFAULT_CHECKPOINT_INTERVAL_MS;

        public String getPath() {
            return this.path;
        }

        public Builder setPath(final String path) {
            this.path = path;
            return this;
        }

        public RaftOptions getRaftOptions() {
            return this.raftOptions;
        }

        public Builder setRaftOptions(final RaftOptions raftOptions) {
            this.raftOptions = raftOptions;
            return this;
        }

        public SkipListSegmentFileLogStorage build() {
            return new SkipListSegmentFileLogStorage(this.path, this.raftOptions, this.maxSegmentFileSize,
                this.preAllocateSegmentCount, this.keepInMemorySegmentCount, this.checkpointIntervalMs,
                this.writeExecutor);
        }

    }

    private final String                segmentsPath;
    private final CheckpointFile        checkpointFile;
    // used  or using segments.
    private List<SegmentFile>           segments;
    // pre-allocated and blank segments.
    private ArrayDeque<AllocatedResult> blankSegments;
    private final Lock                  allocateLock             = new ReentrantLock();
    private final Condition             fullCond                 = this.allocateLock.newCondition();
    private final Condition             emptyCond                = this.allocateLock.newCondition();
    // segment file sequence.
    private final AtomicLong            nextFileSequence         = new AtomicLong(0);
    private final ReadWriteLock         segmentFileReadWriteLock = new ReentrantReadWriteLock();
    private final Lock                  segmentFileWriteLock     = this.segmentFileReadWriteLock.writeLock();
    private final Lock                  segmentFileReadLock      = this.segmentFileReadWriteLock.readLock();
    private ScheduledExecutorService    checkpointExecutor;
    private final AbortFile             abortFile;
    private final ThreadPoolExecutor    writeExecutor;
    private Thread                      segmentAllocator;
    private final int                   maxSegmentFileSize;
    private int                         preAllocateSegmentCount  = PRE_ALLOCATE_SEGMENT_COUNT;
    private int                         keepInMemorySegmentCount = MEM_SEGMENT_COUNT;
    private int                         checkpointIntervalMs     = DEFAULT_CHECKPOINT_INTERVAL_MS;

    /**
     * Creates a RocksDBSegmentLogStorage builder.
     * @return a builder instance.
     */
    public static final Builder builder(final String uri, final RaftOptions raftOptions) {
        return new Builder().setPath(uri).setRaftOptions(raftOptions);
    }

    private static ThreadPoolExecutor createDefaultWriteExecutor() {
        return ThreadPoolUtil.newThreadPool("RocksDBSegmentLogStorage-write-pool", true, Utils.cpus(),
            Utils.cpus() * 3, 60, new ArrayBlockingQueue<>(10000), new NamedThreadFactory(
                "RocksDBSegmentLogStorageWriter"), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public SkipListSegmentFileLogStorage(final String path, final RaftOptions raftOptions,
                                         final int maxSegmentFileSize, final int preAllocateSegmentCount,
                                         final int keepInMemorySegmentCount, final int checkpointIntervalMs,
                                         final ThreadPoolExecutor writeExecutor) {
        this.sync = raftOptions.isSync();
        if (Platform.isMac()) {
            LOG.warn("RocksDBSegmentLogStorage is not recommended on mac os x, it's performance is poorer than RocksDBLogStorage.");
        }
        Requires.requireTrue(maxSegmentFileSize > 0, "maxSegmentFileSize is not greater than zero");
        Requires.requireTrue(preAllocateSegmentCount > 0, "preAllocateSegmentCount is not greater than zero");
        Requires.requireTrue(checkpointIntervalMs > 0, "checkpointIntervalMs is not greater than zero");
        Requires.requireTrue(keepInMemorySegmentCount > 0, "keepInMemorySegmentCount is not greater than zero");
        this.segmentsPath = path + File.separator + "segments";
        this.abortFile = new AbortFile(this.segmentsPath + File.separator + "abort");
        this.checkpointFile = new CheckpointFile(this.segmentsPath + File.separator + "checkpoint");
        this.maxSegmentFileSize = maxSegmentFileSize;
        this.writeExecutor = writeExecutor == null ? createDefaultWriteExecutor() : writeExecutor;
        this.preAllocateSegmentCount = preAllocateSegmentCount;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.keepInMemorySegmentCount = keepInMemorySegmentCount;

    }

    private SegmentFile getLastSegmentFile(final long logIndex, final int waitToWroteSize,
                                           final boolean createIfNecessary, final RocksDBLogStorage.WriteContext ctx)
                                                                                                                     throws IOException,
                                                                                                                     InterruptedException {
        SegmentFile lastFile = null;
        while (true) {
            int segmentCount = 0;
            this.segmentFileReadLock.lock();
            try {

                if (!this.segments.isEmpty()) {
                    segmentCount = this.segments.size();
                    final SegmentFile currLastFile = this.segments.get(this.segments.size() - 1);
                    if (waitToWroteSize <= 0 || !currLastFile.reachesFileEndBy(waitToWroteSize)) {
                        lastFile = currLastFile;
                    }
                }
            } finally {
                this.segmentFileReadLock.unlock();
            }
            if (lastFile == null && createIfNecessary) {
                lastFile = createNewSegmentFile(logIndex, segmentCount, ctx);
                if (lastFile != null) {
                    return lastFile;
                } else {
                    // Try again
                    continue;
                }
            }
            return lastFile;
        }
    }

    private SegmentFile createNewSegmentFile(final long logIndex, final int oldSegmentCount,
                                             final RocksDBLogStorage.WriteContext ctx) throws InterruptedException, IOException {
        SegmentFile segmentFile = null;
        this.segmentFileWriteLock.lock();
        try {
            // CAS by segments count.
            if (this.segments.size() != oldSegmentCount) {
                return segmentFile;
            }
            if (!this.segments.isEmpty()) {
                // Sync current last file and correct it's lastLogIndex.
                final SegmentFile currLastFile = this.segments.get(this.segments.size() - 1);
                currLastFile.setLastLogIndex(logIndex - 1);
                ctx.startJob();
                // Attach a finish hook to set last segment file to be read-only.
                ctx.addFinishHook(() -> currLastFile.setReadOnly(true));
                // Run sync in parallel
                this.writeExecutor.execute(() -> {
                    try {
                        currLastFile.sync(isSync());
                    } catch (final IOException e) {
                        ctx.setError(e);
                    } finally {
                        ctx.finishJob();
                    }

                });

            }
            segmentFile = allocateSegmentFile(logIndex);
            return segmentFile;
        } finally {
            this.segmentFileWriteLock.unlock();
        }

    }

    private SegmentFile allocateSegmentFile(final long index) throws InterruptedException, IOException {
        this.allocateLock.lock();
        try {
            while (this.blankSegments.isEmpty()) {
                this.emptyCond.await();
            }
            final AllocatedResult result = this.blankSegments.pollFirst();
            if (result.ie != null) {
                throw result.ie;
            }
            this.fullCond.signal();
            result.segmentFile.setFirstLogIndex(index);
            this.segments.add(result.segmentFile);
            return result.segmentFile;
        } finally {
            this.allocateLock.unlock();
        }
    }

    private SegmentFile allocateNewSegmentFile() throws IOException {
        SegmentFile segmentFile = new SegmentFile(this.maxSegmentFileSize, getNewSegmentFilePath(), this.writeExecutor);
        final SegmentFileOptions opts = SegmentFileOptions.builder() //
            .setSync(false).setRecover(false).setLastFile(true).setNewFile(true).setPos(0).build();

        if (!segmentFile.init(opts)) {
            throw new IOException("Fail to create new segment file");
        }
        segmentFile.hintLoad();
        LOG.info("Create a new segment file {}.", segmentFile.getPath());
        return segmentFile;
    }

    private String getNewSegmentFilePath() {
        return this.segmentsPath + File.separator + String.format("%019d", this.nextFileSequence.getAndIncrement())
               + SEGMENT_FILE_POSFIX;
    }

    protected void onSync() throws IOException, InterruptedException {
        final SegmentFile lastSegmentFile = getLastSegmentFileForRead();
        if (lastSegmentFile != null) {
            lastSegmentFile.sync(isSync());
        }
    }

    protected boolean isSync() {
        return this.sync;
    }

    private static final Pattern SEGMENT_FILE_NAME_PATTERN = Pattern.compile("[0-9]+\\.s");
    private static final String  FIRST_KEY_FILE_NAME       = "firstKey.conf";

    protected boolean onInitLoaded() {
        final long startMs = Utils.monotonicMs();
        this.segmentFileWriteLock.lock();
        try {
            final File segmentsDir = new File(this.segmentsPath);
            if (!ensureDir(segmentsDir)) {
                return false;
            }
            final Checkpoint checkpoint = loadCheckpoint();

            final File[] segmentFiles = segmentsDir
                    .listFiles((final File dir, final String name) -> SEGMENT_FILE_NAME_PATTERN.matcher(name).matches());

            final boolean normalExit = !this.abortFile.exists();
            if (!normalExit) {
                LOG.info("{} {} did not exit normally, will try to recover last file.", getServiceName(),
                        this.segmentsPath);
            }
            this.segments = new ArrayList<>(segmentFiles == null ? 10 : segmentFiles.length);
            this.blankSegments = new ArrayDeque<>();

            if (segmentFiles != null && segmentFiles.length > 0) {
                // Sort by sequences.
                Arrays.sort(segmentFiles, Comparator.comparing(SkipListSegmentFileLogStorage::getFileSequenceFromFileName));

                final String checkpointSegFile = getCheckpointSegFilePath(checkpoint);

                // mmap files
                for (int i = 0; i < segmentFiles.length; i++) {
                    final File segFile = segmentFiles[i];
                    this.nextFileSequence.set(getFileSequenceFromFileName(segFile) + 1);
                    final SegmentFile segmentFile = new SegmentFile(this.maxSegmentFileSize, segFile.getAbsolutePath(),
                            this.writeExecutor);

                    if (!segmentFile.mmapFile(false)) {
                        LOG.error("Fail to mmap segment file {}.", segFile.getAbsoluteFile());
                        return false;
                    }

                    //load data to ArrayDeque
                    final ByteBuffer readBuffer = segmentFile.getBuffer().asReadOnlyBuffer();
                    int pos = SegmentFile.HEADER_SIZE;
                    readBuffer.position(pos);
                    while(readBuffer.hasRemaining()){
                        int beginPos = pos;
                        if (readBuffer.remaining() < SegmentFile.RECORD_MAGIC_BYTES_SIZE) {
                            throw new IOException("Missing magic buffer.");
                        }
                        int offset = 0;
                        boolean judge = true;
                        for (; offset < SegmentFile.RECORD_MAGIC_BYTES_SIZE; offset++) {
                            if (readBuffer.get(pos+offset) != SegmentFile.RECORD_MAGIC_BYTES[offset]) {
                                judge = false;
                            }
                        }
                        if (!judge){
                            break;
                        }
                        readBuffer.position(pos + SegmentFile.RECORD_MAGIC_BYTES_SIZE);
                        final int dataLen = readBuffer.getInt();
                        final byte[] data = new byte[dataLen];
                        readBuffer.get(data);
                        final LogEntry entry = this.logEntryDecoder.decode(data);
                        if (entry != null) {
                            byte[] keyBytes = getKeyBytes(entry.getId().getIndex());
                            byte[] metadata = encodeLocationMetadata(segmentFile.getFirstLogIndex(), beginPos);
                            if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION){
                                addConfPure(keyBytes,metadata);
                            }
                            addDataPure(keyBytes,metadata);
                        } else {
                            LOG.error("load data to ArrayDeque fail");
                        }
                        pos += (SegmentFile.RECORD_MAGIC_BYTES_SIZE + SegmentFile.RECORD_DATA_LENGTH_SIZE + dataLen);
                        readBuffer.position(pos);
                    }
                    if (segmentFile.isBlank()) {
                        this.blankSegments.add(new AllocatedResult(segmentFile));
                    } else {
                        this.segments.add(segmentFile);
                    }
                }

                //load FIRST_LOG_IDX to conf log
                File file = new File(this.segmentsPath,FIRST_KEY_FILE_NAME);
                if (!file.exists()) {
                    try {
                        boolean b = file.createNewFile();
                        if (!b){
                            LOG.error("Create firstKey file error.");
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }else{
                    try (InputStream inputStream = new FileInputStream(file)){
                        byte[] firstKey = new byte[8];
                        int read = inputStream.read(firstKey);
                        if (read != -1){
                            putFirstKeyBytes(firstKey);
                        }
                    }
                }

                // init blank segments
                for (AllocatedResult ret : this.blankSegments) {
                    final SegmentFile segmentFile = ret.segmentFile;
                    final SegmentFileOptions opts = SegmentFileOptions.builder().setSync(false).build();
                    if (!segmentFile.init(opts)) {
                        LOG.error("Fail to load blank segment file {}.", segmentFile.getPath());
                        segmentFile.shutdown();
                        return false;
                    }
                }

                // try to recover segments
                boolean needRecover = false;
                SegmentFile prevFile = null;
                for (int i = 0; i < this.segments.size(); i++) {
                    final boolean isLastFile = i == this.segments.size() - 1;
                    SegmentFile segmentFile = this.segments.get(i);
                    int pos = segmentFile.getSize();
                    if (StringUtil.equalsIgnoreCase(checkpointSegFile, segmentFile.getFilename())) {
                        needRecover = true;
                        assert (checkpoint != null);
                        pos = checkpoint.committedPos;
                    } else {
                        if (needRecover) {
                            pos = 0;
                        }
                    }

                    final SegmentFileOptions opts = SegmentFileOptions.builder() //
                            .setSync(isSync()) //
                            .setRecover(needRecover && !normalExit) //
                            .setLastFile(isLastFile) //
                            .setNewFile(false) //
                            .setPos(pos).build();

                    if (!segmentFile.init(opts)) {
                        LOG.error("Fail to load segment file {}.", segmentFile.getPath());
                        segmentFile.shutdown();
                        return false;
                    }
                    /**
                     * It's wrote position is from start(HEADER_SIZE) but it's not the last file, SHOULD not happen.
                     */
                    if (segmentFile.getWrotePos() == SegmentFile.HEADER_SIZE && !isLastFile) {
                        LOG.error("Detected corrupted segment file {}.", segmentFile.getPath());
                        return false;
                    }

                    if (prevFile != null) {
                        prevFile.setLastLogIndex(segmentFile.getFirstLogIndex() - 1);
                    }
                    prevFile = segmentFile;
                }
                if (getLastLogIndex() > 0 && prevFile != null) {
                    prevFile.setLastLogIndex(getLastLogIndex());
                }
            } else {
                if (checkpoint != null) {
                    LOG.warn("Missing segment files, checkpoint is: {}", checkpoint);
                    return false;
                }
            }

            LOG.info("{} Loaded {} segment files and  {} blank segment files from path {}.", getServiceName(),
                    this.segments.size(), this.blankSegments.size(), this.segmentsPath);

            LOG.info("{} segments: \n{}", getServiceName(), descSegments());

            startCheckpointTask();

            if (normalExit) {
                if (!this.abortFile.create()) {
                    LOG.error("Fail to create abort file {}.", this.abortFile.getPath());
                    return false;
                }
            } else {
                this.abortFile.touch();
            }
            startSegmentAllocator();

            return true;
        } catch (final Exception e) {
            LOG.error("Fail to load segment files from directory {}.", this.segmentsPath, e);
            return false;
        } finally {
            this.segmentFileWriteLock.unlock();
            LOG.info("{} init and load cost {} ms.", getServiceName(), Utils.monotonicMs() - startMs);
        }
    }

    private void startSegmentAllocator() throws IOException {
        // Warmup
        if (this.blankSegments.isEmpty()) {
            doAllocateSegment0();
        }
        // Start the thread.
        this.segmentAllocator = new Thread() {
            @Override
            public void run() {
                doAllocateSegment();
            }

        };
        this.segmentAllocator.setDaemon(true);
        this.segmentAllocator.setName("SegmentAllocator");
        this.segmentAllocator.start();
    }

    private void doAllocateSegment() {
        LOG.info("SegmentAllocator is started.");
        while (!Thread.currentThread().isInterrupted()) {
            doAllocateSegmentInLock();
            doSwappOutSegments();
        }
        LOG.info("SegmentAllocator exit.");
    }

    private void doAllocateSegmentInLock() {
        this.allocateLock.lock();
        try {
            //TODO configure cap
            while (this.blankSegments.size() >= this.preAllocateSegmentCount) {
                this.fullCond.await();
            }
            doAllocateSegment0();
            this.emptyCond.signal();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final IOException e) {
            this.blankSegments.add(new AllocatedResult(e));
            this.emptyCond.signal();
        } finally {
            this.allocateLock.unlock();
        }
    }

    private void doSwappOutSegments() {
        this.segmentFileReadLock.lock();
        try {
            if (this.segments.size() <= this.keepInMemorySegmentCount) {
                return;
            }
            int segmentsInMemeCount = 0;
            int swappedOutCount = 0;
            final long beginTime = Utils.monotonicMs();
            final int lastIndex = this.segments.size() - 1;
            for (int i = lastIndex; i >= 0; i--) {
                SegmentFile segFile = this.segments.get(i);
                if (!segFile.isSwappedOut()) {
                    segmentsInMemeCount++;
                    if (segmentsInMemeCount >= this.keepInMemorySegmentCount && i != lastIndex) {
                        segFile.hintUnload();
                        segFile.swapOut();
                        swappedOutCount++;
                    }
                }
            }
            LOG.info("Swapped out {} segment files, cost {} ms.", swappedOutCount, Utils.monotonicMs() - beginTime);
        } catch (final Exception e) {
            LOG.error("Fail to swap out segments.", e);
        } finally {
            this.segmentFileReadLock.unlock();
        }
    }

    private void doAllocateSegment0() throws IOException {
        SegmentFile segFile = allocateNewSegmentFile();
        this.blankSegments.add(new AllocatedResult(segFile));
    }

    private static long getFileSequenceFromFileName(final File file) {
        final String name = file.getName();
        assert (name.endsWith(SEGMENT_FILE_POSFIX));
        int idx = name.indexOf(SEGMENT_FILE_POSFIX);
        return Long.valueOf(name.substring(0, idx));
    }

    private Checkpoint loadCheckpoint() {
        final Checkpoint checkpoint;
        try {
            checkpoint = this.checkpointFile.load();
            if (checkpoint != null) {
                LOG.info("Loaded checkpoint: {} from {}.", checkpoint, this.checkpointFile.getPath());
            }
        } catch (final IOException e) {
            LOG.error("Fail to load checkpoint file: {}", this.checkpointFile.getPath(), e);
            return null;
        }
        return checkpoint;
    }

    private boolean ensureDir(final File segmentsDir) {
        try {
            FileUtils.forceMkdir(segmentsDir);
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to create segments directory: {}", this.segmentsPath, e);
            return false;
        }
    }

    private String getCheckpointSegFilePath(final Checkpoint checkpoint) {
        return checkpoint != null ? checkpoint.segFilename : null;
    }

    private void startCheckpointTask() {
        this.checkpointExecutor = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory(getServiceName() + "-Checkpoint-Thread-", true));
        this.checkpointExecutor.scheduleAtFixedRate(this::doCheckpoint, this.checkpointIntervalMs,
                this.checkpointIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("{} started checkpoint task.", getServiceName());
    }

    private StringBuilder descSegments() {
        final StringBuilder segmentsDesc = new StringBuilder("[\n");
        for (final SegmentFile segFile : this.segments) {
            segmentsDesc.append("  ").append(segFile.toString()).append("\n");
        }
        segmentsDesc.append("]");
        return segmentsDesc;
    }

    private String getServiceName() {
        return this.getClass().getSimpleName();
    }

    private void stopSegmentAllocator() {
        this.segmentAllocator.interrupt();
        try {
            this.segmentAllocator.join(500);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void onShutdown() {
        stopCheckpointTask();
        stopSegmentAllocator();

        List<SegmentFile> shutdownFiles = Collections.emptyList();
        this.segmentFileWriteLock.lock();
        try {
            doCheckpoint();
            shutdownFiles = new ArrayList<>(this.segments);
            this.segments.clear();
            if (!this.abortFile.destroy()) {
                LOG.error("Fail to delete abort file {}.", this.abortFile.getPath());
            }
        } finally {
            this.segmentFileWriteLock.unlock();
            for (final SegmentFile segmentFile : shutdownFiles) {
                segmentFile.shutdown();
            }
            shutdownBlankSegments();
        }
        this.writeExecutor.shutdown();
    }

    private void shutdownBlankSegments() {
        this.allocateLock.lock();
        try {
            for (final AllocatedResult ret : this.blankSegments) {
                if (ret.segmentFile != null) {
                    ret.segmentFile.shutdown();
                }
            }
        } finally {
            this.allocateLock.unlock();
        }
    }

    private void stopCheckpointTask() {
        if (this.checkpointExecutor != null) {
            this.checkpointExecutor.shutdownNow();
            try {
                this.checkpointExecutor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOG.info("{} stopped checkpoint task.", getServiceName());
        }
    }

    private void doCheckpoint() {
        SegmentFile lastSegmentFile = null;
        try {
            lastSegmentFile = getLastSegmentFileForRead();
            if (lastSegmentFile != null) {
                this.checkpointFile.save(new Checkpoint(lastSegmentFile.getFilename(), lastSegmentFile
                    .getCommittedPos()));
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final IOException e) {
            LOG.error("Fatal error, fail to do checkpoint, last segment file is {}.",
                lastSegmentFile != null ? lastSegmentFile.getPath() : "null", e);
        }
    }

    private SegmentFile getLastSegmentFileForRead() throws IOException, InterruptedException {
        return getLastSegmentFile(-1, 0, false, null);
    }

    protected void onReset(final long nextLogIndex) {
        List<SegmentFile> destroyedFiles = new ArrayList<>();
        this.segmentFileWriteLock.lock();
        try {
            File file = new File(this.segmentsPath, FIRST_KEY_FILE_NAME);
            if (file.exists()) {
                file.delete();
            }
            this.checkpointFile.destroy();
            destroyedFiles.addAll(this.segments);
            this.segments.clear();
            LOG.info("Destroyed segments and checkpoint in path {} by resetting.", this.segmentsPath);
        } finally {
            this.segmentFileWriteLock.unlock();
            for (SegmentFile segFile : destroyedFiles) {
                segFile.destroy();
            }
        }
    }

    protected void onTruncatePrefix(final long startIndex, final long firstIndexKept) {
        List<SegmentFile> destroyedFiles = null;
        this.segmentFileWriteLock.lock();
        try {
            int fromIndex = binarySearchFileIndexByLogIndex(startIndex);
            final int toIndex = binarySearchFileIndexByLogIndex(firstIndexKept);

            if (fromIndex < 0) {
                fromIndex = 0;
            }
            if (toIndex < 0) {
                return;
            }

            final List<SegmentFile> removedFiles = this.segments.subList(fromIndex, toIndex);
            destroyedFiles = new ArrayList<>(removedFiles);
            removedFiles.clear();
            doCheckpoint();
        } finally {
            this.segmentFileWriteLock.unlock();
            if (destroyedFiles != null) {
                for (final SegmentFile segmentFile : destroyedFiles) {
                    segmentFile.destroy();
                }
            }
        }
    }

    private boolean isMetadata(final byte[] data) {
        for (int offset = 0; offset < SegmentFile.RECORD_MAGIC_BYTES_SIZE; offset++) {
            if (data[offset] != SegmentFile.RECORD_MAGIC_BYTES[offset]) {
                return false;
            }
        }
        return true;
    }

    protected void onTruncateSuffix(final long lastIndexKept) throws IOException {
        List<SegmentFile> destroyedFiles = null;
        this.segmentFileWriteLock.lock();
        try {
            final int keptFileIndex = binarySearchFileIndexByLogIndex(lastIndexKept);
            int toIndex = binarySearchFileIndexByLogIndex(getLastLogIndex());

            if (keptFileIndex < 0) {
                LOG.warn("Segment file not found by logIndex={} to be truncate_suffix, current segments:\n{}.",
                    lastIndexKept, descSegments());
                return;
            }

            if (toIndex < 0) {
                toIndex = this.segments.size() - 1;
            }

            // Destroyed files after keptFile
            final List<SegmentFile> removedFiles = this.segments.subList(keptFileIndex + 1, toIndex + 1);
            destroyedFiles = new ArrayList<>(removedFiles);
            removedFiles.clear();

            // Process logs in keptFile(firstLogIndex=lastIndexKept)
            final SegmentFile keptFile = this.segments.get(keptFileIndex);
            if (keptFile.isBlank()) {
                return;
            }
            int logWrotePos = -1; // The truncate position in keptFile.

            // Try to find the right position to be truncated.
            {
                // First, find in right [lastIndexKept + 1, getLastLogIndex()]
                long nextIndex = lastIndexKept + 1;
                final long endIndex = Math.min(getLastLogIndex(), keptFile.getLastLogIndex());
                while (nextIndex <= endIndex) {
                    final byte[] data = getValueFromSkipList(nextIndex);
                    if (data != null) {
                        if (data.length == LOCATION_METADATA_SIZE) {
                            if (!isMetadata(data)) {
                                // Stored in rocksdb directly.
                                nextIndex++;
                                continue;
                            }
                            logWrotePos = getWrotePosition(data);
                            break;
                        } else {
                            // Stored in rocksdb directly.
                            nextIndex++;
                        }
                    } else {
                        // No more data.
                        break;
                    }
                }
            }

            // Not found in [lastIndexKept + 1, getLastLogIndex()]
            if (logWrotePos < 0) {
                // Second, try to find in left  [firstLogIndex, lastIndexKept) when lastIndexKept is not stored in segments.
                final byte[] keptData = getValueFromSkipList(lastIndexKept);
                // The kept log's data is not stored in segments.
                if (!isMetadata(keptData)) {
                    //lastIndexKept's log is stored in rocksdb directly, try to find the first previous log that stored in segment.
                    long prevIndex = lastIndexKept - 1;
                    final long startIndex = keptFile.getFirstLogIndex();
                    while (prevIndex >= startIndex) {
                        final byte[] data = getValueFromSkipList(prevIndex);
                        if (data != null) {
                            if (data.length == LOCATION_METADATA_SIZE) {
                                if (!isMetadata(data)) {
                                    // Stored in rocksdb directly.
                                    prevIndex--;
                                    continue;
                                }
                                // Found the position.
                                logWrotePos = getWrotePosition(data);
                                final byte[] logData = onDataGet(prevIndex, data);
                                // Skip this log, it should be kept(logs that are less than lastIndexKept should be kept).
                                // Fine the next log position.
                                logWrotePos += SegmentFile.getWriteBytes(logData);
                                break;
                            } else {
                                // Stored in rocksdb directly.
                                prevIndex--;
                            }
                        } else {
                            LOG.warn(
                                "Log entry not found at index={} when truncating logs suffix from lastIndexKept={}.",
                                prevIndex, lastIndexKept);
                            prevIndex--;
                        }
                    }
                }
            }

            if (logWrotePos >= 0 && logWrotePos < keptFile.getSize()) {
                // Truncate the file from wrotePos and set it's lastLogIndex=lastIndexKept.
                keptFile.truncateSuffix(logWrotePos, lastIndexKept, isSync());
            }
            // Finally, do checkpoint.
            doCheckpoint();

        } finally {
            this.segmentFileWriteLock.unlock();
            if (destroyedFiles != null) {
                for (final SegmentFile segmentFile : destroyedFiles) {
                    segmentFile.destroy();
                }
            }
        }
    }

    /**
     * Retrieve the log wrote position from metadata.
     *
     * @param data the metadata
     * @return the log wrote position
     */
    private int getWrotePosition(final byte[] data) {
        return Bits.getInt(data, SegmentFile.RECORD_MAGIC_BYTES_SIZE + 2 + 8);
    }

    protected RocksDBLogStorage.WriteContext newWriteContext() {
        return new BarrierWriteContext();
    }

    protected byte[] onDataAppend(final long logIndex, final byte[] value, final RocksDBLogStorage.WriteContext ctx)
                                                                                                                    throws IOException,
                                                                                                                    InterruptedException {
        SegmentFile lastSegmentFile = getLastSegmentFile(logIndex, SegmentFile.getWriteBytes(value), true, ctx);
        //        if (value.length < this.valueSizeThreshold) {
        //            // Small value will be stored in rocksdb directly.
        //            lastSegmentFile.setLastLogIndex(logIndex);
        //            ctx.finishJob();
        //            return value;
        //        }
        // Large value is stored in segment file and returns an encoded location info that will be stored in rocksdb.
        final int pos = lastSegmentFile.write(logIndex, value, (RocksDBLogStorage.WriteContext) ctx);
        final long firstLogIndex = lastSegmentFile.getFirstLogIndex();
        return encodeLocationMetadata(firstLogIndex, pos);
    }

    /**
     * Encode segment file firstLogIndex(fileName) and position to a byte array in the format of:
     * <ul>
     *  <li> magic bytes(2 B)</li>
     *  <li> reserved (2 B)</li>
     *  <li> firstLogIndex(8 B)</li>
     *  <li> wrote position(4 B)</li>
     * </ul>
     * @param firstLogIndex the first log index
     * @param pos           the wrote position
     * @return segment info
     */
    private byte[] encodeLocationMetadata(final long firstLogIndex, final int pos) {
        final byte[] newData = new byte[LOCATION_METADATA_SIZE];
        System.arraycopy(SegmentFile.RECORD_MAGIC_BYTES, 0, newData, 0, SegmentFile.RECORD_MAGIC_BYTES_SIZE);
        // 2 bytes reserved
        Bits.putLong(newData, SegmentFile.RECORD_MAGIC_BYTES_SIZE + 2, firstLogIndex);
        Bits.putInt(newData, SegmentFile.RECORD_MAGIC_BYTES_SIZE + 2 + 8, pos);
        return newData;
    }

    private int binarySearchFileIndexByLogIndex(final long logIndex) {
        this.segmentFileReadLock.lock();
        try {
            if (this.segments.isEmpty()) {
                return -1;
            }
            if (this.segments.size() == 1) {
                final SegmentFile firstFile = this.segments.get(0);
                if (firstFile.contains(logIndex)) {
                    return 0;
                } else {
                    return -1;
                }
            }

            int low = 0;
            int high = this.segments.size() - 1;

            while (low <= high) {
                final int mid = (low + high) >>> 1;

                final SegmentFile file = this.segments.get(mid);
                if (file.getLastLogIndex() < logIndex) {
                    low = mid + 1;
                } else if (file.getFirstLogIndex() > logIndex) {
                    high = mid - 1;
                } else {
                    return mid;
                }
            }
            return -(low + 1);
        } finally {
            this.segmentFileReadLock.unlock();
        }
    }

    private SegmentFile binarySearchFileByFirstLogIndex(final long logIndex) {
        this.segmentFileReadLock.lock();
        try {
            if (this.segments.isEmpty()) {
                return null;
            }
            if (this.segments.size() == 1) {
                final SegmentFile firstFile = this.segments.get(0);
                if (firstFile.getFirstLogIndex() == logIndex) {
                    return firstFile;
                } else {
                    return null;
                }
            }

            int low = 0;
            int high = this.segments.size() - 1;

            while (low <= high) {
                final int mid = (low + high) >>> 1;

                final SegmentFile file = this.segments.get(mid);
                if (file.getFirstLogIndex() < logIndex) {
                    low = mid + 1;
                } else if (file.getFirstLogIndex() > logIndex) {
                    high = mid - 1;
                } else {
                    return file;
                }
            }
            return null;
        } finally {
            this.segmentFileReadLock.unlock();
        }
    }

    protected byte[] onDataGet(final long logIndex, final byte[] value) throws IOException {
        if (value == null || value.length != LOCATION_METADATA_SIZE) {
            return value;
        }

        int offset = 0;
        for (; offset < SegmentFile.RECORD_MAGIC_BYTES_SIZE; offset++) {
            if (value[offset] != SegmentFile.RECORD_MAGIC_BYTES[offset]) {
                return value;
            }
        }

        // skip reserved
        offset += 2;

        final long firstLogIndex = Bits.getLong(value, offset);
        final int pos = Bits.getInt(value, offset + 8);
        final SegmentFile file = binarySearchFileByFirstLogIndex(firstLogIndex);
        if (file == null) {
            return null;
        }
        return file.read(logIndex, pos);
    }

    protected byte[] onFirstLogIndexAppend(byte[] vs) {
        try (OutputStream outputStream = new FileOutputStream(new File(this.segmentsPath, FIRST_KEY_FILE_NAME))) {
            outputStream.write(vs);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return vs;
    }
}
