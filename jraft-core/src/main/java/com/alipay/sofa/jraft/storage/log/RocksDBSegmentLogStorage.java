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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
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

import org.apache.commons.io.FileUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.common.profile.StringUtil;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.storage.log.CheckpointFile.Checkpoint;
import com.alipay.sofa.jraft.storage.log.SegmentFile.SegmentFileOptions;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.CountDownEvent;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Platform;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Log Storage implementation based on rocksdb and segment files.
 *
 * @author boyan(boyan@antfin.com)
 */
public class RocksDBSegmentLogStorage extends RocksDBLogStorage {

    private static final int PRE_ALLOCATE_SEGMENT_COUNT = 2;
    private static final int MEM_SEGMENT_COUNT          = 3;

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

    public static class BarrierWriteContext implements WriteContext {
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
                                                                   .getLogger(RocksDBSegmentLogStorage.class);

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

    // Default value size threshold to decide whether it will be stored in segments or rocksdb, default is 4K.
    // When the value size is less than 4K, it will be stored in rocksdb directly.
    private static int          DEFAULT_VALUE_SIZE_THRESHOLD   = SystemPropertyUtil.getInt(
                                                                   "jraft.log_storage.segment.value.threshold.bytes",
                                                                   4 * 1024);

    /**
     * RocksDBSegmentLogStorage builder
     * @author boyan(boyan@antfin.com)
     *
     */
    public static class Builder {
        private String             path;
        private RaftOptions        raftOptions;
        private int                valueSizeThreshold       = DEFAULT_VALUE_SIZE_THRESHOLD;
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

        public int getValueSizeThreshold() {
            return this.valueSizeThreshold;
        }

        public Builder setValueSizeThreshold(final int valueSizeThreshold) {
            this.valueSizeThreshold = valueSizeThreshold;
            return this;
        }

        public int getMaxSegmentFileSize() {
            return this.maxSegmentFileSize;
        }

        public Builder setMaxSegmentFileSize(final int maxSegmentFileSize) {
            this.maxSegmentFileSize = maxSegmentFileSize;
            return this;
        }

        public ThreadPoolExecutor getWriteExecutor() {
            return this.writeExecutor;
        }

        public Builder setWriteExecutor(final ThreadPoolExecutor writeExecutor) {
            this.writeExecutor = writeExecutor;
            return this;
        }

        public int getPreAllocateSegmentCount() {
            return this.preAllocateSegmentCount;
        }

        public Builder setPreAllocateSegmentCount(final int preAllocateSegmentCount) {
            this.preAllocateSegmentCount = preAllocateSegmentCount;
            return this;
        }

        public int getKeepInMemorySegmentCount() {
            return this.keepInMemorySegmentCount;
        }

        public Builder setKeepInMemorySegmentCount(final int keepInMemorySegmentCount) {
            this.keepInMemorySegmentCount = keepInMemorySegmentCount;
            return this;
        }

        public int getCheckpointIntervalMs() {
            return this.checkpointIntervalMs;
        }

        public Builder setCheckpointIntervalMs(final int checkpointIntervalMs) {
            this.checkpointIntervalMs = checkpointIntervalMs;
            return this;
        }

        public RocksDBSegmentLogStorage build() {
            return new RocksDBSegmentLogStorage(this.path, this.raftOptions, this.valueSizeThreshold,
                this.maxSegmentFileSize, this.preAllocateSegmentCount, this.keepInMemorySegmentCount,
                this.checkpointIntervalMs, this.writeExecutor);
        }

    }

    private final int                   valueSizeThreshold;
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
    private final ReadWriteLock         readWriteLock            = new ReentrantReadWriteLock();
    private final Lock                  writeLock                = this.readWriteLock.writeLock();
    private final Lock                  readLock                 = this.readWriteLock.readLock();
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

    public RocksDBSegmentLogStorage(final String path, final RaftOptions raftOptions) {
        this(path, raftOptions, DEFAULT_VALUE_SIZE_THRESHOLD, MAX_SEGMENT_FILE_SIZE);
    }

    public RocksDBSegmentLogStorage(final String path, final RaftOptions raftOptions, final int valueSizeThreshold,
                                    final int maxSegmentFileSize) {
        this(path, raftOptions, valueSizeThreshold, maxSegmentFileSize, PRE_ALLOCATE_SEGMENT_COUNT, MEM_SEGMENT_COUNT,
            DEFAULT_CHECKPOINT_INTERVAL_MS, createDefaultWriteExecutor());
    }

    private static ThreadPoolExecutor createDefaultWriteExecutor() {
        return ThreadPoolUtil.newThreadPool("RocksDBSegmentLogStorage-write-pool", true, Utils.cpus(),
            Utils.cpus() * 3, 60, new ArrayBlockingQueue<>(10000), new NamedThreadFactory(
                "RocksDBSegmentLogStorageWriter"), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public RocksDBSegmentLogStorage(final String path, final RaftOptions raftOptions, final int valueSizeThreshold,
                                    final int maxSegmentFileSize, final int preAllocateSegmentCount,
                                    final int keepInMemorySegmentCount, final int checkpointIntervalMs,
                                    final ThreadPoolExecutor writeExecutor) {
        super(path, raftOptions);
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
        this.valueSizeThreshold = valueSizeThreshold;
        this.maxSegmentFileSize = maxSegmentFileSize;
        this.writeExecutor = writeExecutor == null ? createDefaultWriteExecutor() : writeExecutor;
        this.preAllocateSegmentCount = preAllocateSegmentCount;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.keepInMemorySegmentCount = keepInMemorySegmentCount;

    }

    private SegmentFile getLastSegmentFile(final long logIndex, final int waitToWroteSize,
                                           final boolean createIfNecessary, final WriteContext ctx) throws IOException,
                                                                                                   InterruptedException {
        SegmentFile lastFile = null;
        while (true) {
            int segmentCount = 0;
            this.readLock.lock();
            try {

                if (!this.segments.isEmpty()) {
                    segmentCount = this.segments.size();
                    final SegmentFile currLastFile = getLastSegmentWithoutLock();
                    if (waitToWroteSize <= 0 || !currLastFile.reachesFileEndBy(waitToWroteSize)) {
                        lastFile = currLastFile;
                    }
                }
            } finally {
                this.readLock.unlock();
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
                                             final WriteContext ctx) throws InterruptedException, IOException {
        SegmentFile segmentFile = null;
        this.writeLock.lock();
        try {
            // CAS by segments count.
            if (this.segments.size() != oldSegmentCount) {
                return segmentFile;
            }
            if (!this.segments.isEmpty()) {
                // Sync current last file and correct it's lastLogIndex.
                final SegmentFile currLastFile = getLastSegmentWithoutLock();
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
            this.writeLock.unlock();
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
        final String newSegPath = getNewSegmentFilePath();
        SegmentFile segmentFile = new SegmentFile(this.maxSegmentFileSize, newSegPath, this.writeExecutor);
        final SegmentFileOptions opts = SegmentFileOptions.builder() //
            .setSync(false) //
            .setRecover(false) //
            .setLastFile(true) //
            .setNewFile(true) //
            .setPos(0).build();

        try {
            if (!segmentFile.init(opts)) {
                throw new IOException("Fail to create new segment file");
            }
            segmentFile.hintLoad();
            LOG.info("Create a new segment file {}.", segmentFile.getPath());
            return segmentFile;
        } catch (IOException e) {
            // Delete the file if fails
            FileUtils.deleteQuietly(new File(newSegPath));
            throw e;
        }
    }

    private String getNewSegmentFilePath() {
        return this.segmentsPath + File.separator + String.format("%019d", this.nextFileSequence.getAndIncrement())
               + SEGMENT_FILE_POSFIX;
    }

    @Override
    protected void onSync() throws IOException, InterruptedException {
        final SegmentFile lastSegmentFile = getLastSegmentFileForRead();
        if (lastSegmentFile != null) {
            lastSegmentFile.sync(isSync());
        }
    }

    private static final Pattern SEGMENT_FILE_NAME_PATTERN = Pattern.compile("[0-9]+\\.s");

    @Override
    protected boolean onInitLoaded() {
        final long startMs = Utils.monotonicMs();
        this.writeLock.lock();
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
            List<File> corruptedHeaderSegments = new ArrayList<>();

            if (segmentFiles != null && segmentFiles.length > 0) {
                // Sort by sequences.
                Arrays.sort(segmentFiles, Comparator.comparing(RocksDBSegmentLogStorage::getFileSequenceFromFileName));

                final String checkpointSegFile = getCheckpointSegFilePath(checkpoint);

                // mmap files
                for (int i = 0; i < segmentFiles.length; i++) {
                    final File segFile = segmentFiles[i];
                    this.nextFileSequence.set(getFileSequenceFromFileName(segFile) + 1);
                    final SegmentFile segmentFile = new SegmentFile(this.maxSegmentFileSize, segFile.getAbsolutePath(),
                        this.writeExecutor);

                    if (!segmentFile.mmapFile(false)) {
                      assert (segmentFile.isHeaderCorrupted());
                      corruptedHeaderSegments.add(segFile);
                      continue;
                    }

                    if (segmentFile.isBlank()) {
                      this.blankSegments.add(new AllocatedResult(segmentFile));
                    } else if (segmentFile.isHeaderCorrupted()) {
                      corruptedHeaderSegments.add(segFile);
                    } else {
                      this.segments.add(segmentFile);
                    }
                }

                // Processing corrupted header files
                //TODO(boyan) maybe we can find a better solution for such case that new allocated segment file is corrupted when power failure etc.
                if(!processCorruptedHeaderFiles(corruptedHeaderSegments)) {
                  return false;
                }

                // init blank segments
                if(!initBlankFiles()) {
                  return false;
                }

                // try to recover segments
                if(!recoverFiles(checkpoint, normalExit, checkpointSegFile)) {
                  return false;
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
            this.writeLock.unlock();
            LOG.info("{} init and load cost {} ms.", getServiceName(), Utils.monotonicMs() - startMs);
        }
    }

    private boolean recoverFiles(final Checkpoint checkpoint, final boolean normalExit, final String checkpointSegFile) {
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
        return true;
    }

    private boolean initBlankFiles() {
        for (AllocatedResult ret : this.blankSegments) {
            final SegmentFile segmentFile = ret.segmentFile;
            final SegmentFileOptions opts = SegmentFileOptions.builder() //
                .setSync(false) //
                .setRecover(false) //
                .setLastFile(true) //
                .build();

            if (!segmentFile.init(opts)) {
                LOG.error("Fail to load blank segment file {}.", segmentFile.getPath());
                segmentFile.shutdown();
                return false;
            }
        }
        return true;
    }

    private boolean processCorruptedHeaderFiles(final List<File> corruptedHeaderSegments) throws IOException {
        if (corruptedHeaderSegments.size() == 1) {
            final File corruptedFile = corruptedHeaderSegments.get(0);
            if (getFileSequenceFromFileName(corruptedFile) != this.nextFileSequence.get() - 1) {
                LOG.error("Detected corrupted header segment file {}.", corruptedFile);
                return false;
            } else {
                // The file is the last file,it's the new blank segment but fail to save header, we can
                // remove it safely.
                LOG.warn("Truncate the last segment file {} which it's header is corrupted.",
                    corruptedFile.getAbsolutePath());
                // We don't want to delete it, but rename it for safety.
                FileUtils.moveFile(corruptedFile, new File(corruptedFile.getAbsolutePath() + ".corrupted"));
            }
        } else if (corruptedHeaderSegments.size() > 1) {
            // FATAL: it should not happen.
            LOG.error("Detected corrupted header segment files: {}.", corruptedHeaderSegments);
            return false;
        }

        return true;
    }

    private void startSegmentAllocator() throws IOException {
        // Warmup
        if (this.blankSegments.isEmpty()) {
            doAllocateSegment0();
        }
        // Start the thread.
        this.segmentAllocator = new Thread(this::doAllocateSegment);
        this.segmentAllocator.setDaemon(true);
        this.segmentAllocator.setName("SegmentAllocator");
        this.segmentAllocator.start();
    }

    private void doAllocateSegment() {
        LOG.info("SegmentAllocator is started.");
        while (!Thread.currentThread().isInterrupted()) {
            doAllocateSegmentInLock();
            doSwapOutSegments(false);
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

    private void doSwapOutSegments(final boolean force) {
        if (force) {
            this.readLock.lock();
        } else if (!this.readLock.tryLock()) {
            return;
        }
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
            this.readLock.unlock();
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

    @Override
    protected void onShutdown() {
        stopCheckpointTask();
        stopSegmentAllocator();

        List<SegmentFile> shutdownFiles = Collections.emptyList();
        this.writeLock.lock();
        try {
            doCheckpoint();
            shutdownFiles = new ArrayList<>(this.segments);
            this.segments.clear();
            if (!this.abortFile.destroy()) {
                LOG.error("Fail to delete abort file {}.", this.abortFile.getPath());
            }
        } finally {
            this.writeLock.unlock();
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

    public SegmentFile getLastSegmentFileForRead() throws IOException, InterruptedException {
        return getLastSegmentFile(-1, 0, false, null);
    }

    @Override
    protected void onReset(final long nextLogIndex) {
        List<SegmentFile> destroyedFiles = new ArrayList<>();
        this.writeLock.lock();
        try {
            this.checkpointFile.destroy();
            destroyedFiles.addAll(this.segments);
            this.segments.clear();
            LOG.info("Destroyed segments and checkpoint in path {} by resetting.", this.segmentsPath);
        } finally {
            this.writeLock.unlock();
            for (SegmentFile segFile : destroyedFiles) {
                segFile.destroy();
            }
        }
    }

    private SegmentFile getLastSegmentWithoutLock() {
        return this.segments.get(this.segments.size() - 1);
    }

    @Override
    protected void onTruncatePrefix(final long startIndex, final long firstIndexKept) throws RocksDBException,
                                                                                     IOException {
        List<SegmentFile> destroyedFiles = null;
        this.writeLock.lock();
        try {
            int fromIndex = binarySearchFileIndexByLogIndex(startIndex);
            int toIndex = binarySearchFileIndexByLogIndex(firstIndexKept);

            if (fromIndex < 0) {
                fromIndex = 0;
            }
            if (toIndex < 0) {
                // When all the segments contain logs that index is smaller than firstIndexKept,
                // truncate all segments.
                do {
                    if (!this.segments.isEmpty()) {
                        if (getLastSegmentWithoutLock().getLastLogIndex() < firstIndexKept) {
                            toIndex = this.segments.size();
                            break;
                        }
                    }
                    LOG.warn("Segment file not found by logIndex={} to be truncate_prefix, current segments:\n{}.",
                        firstIndexKept, descSegments());
                    return;
                } while (false);
            }

            final List<SegmentFile> removedFiles = this.segments.subList(fromIndex, toIndex);
            destroyedFiles = new ArrayList<>(removedFiles);
            removedFiles.clear();
            doCheckpoint();
        } finally {
            this.writeLock.unlock();
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

    private SegmentFile getFirstSegmentWithoutLock() {
        return this.segments.get(0);
    }

    @Override
    protected void onTruncateSuffix(final long lastIndexKept) throws RocksDBException, IOException {
        List<SegmentFile> destroyedFiles = null;
        this.writeLock.lock();
        try {
            final int keptFileIndex = binarySearchFileIndexByLogIndex(lastIndexKept);
            int toIndex = binarySearchFileIndexByLogIndex(getLastLogIndex());

            if (keptFileIndex < 0) {
                // When all the segments contain logs that index is greater than lastIndexKept,
                // truncate all segments.
                if (!this.segments.isEmpty()) {
                    final long firstLogIndex = getFirstSegmentWithoutLock().getFirstLogIndex();
                    if (firstLogIndex > lastIndexKept) {
                        final List<SegmentFile> removedFiles = this.segments.subList(0, this.segments.size());
                        destroyedFiles = new ArrayList<>(removedFiles);
                        removedFiles.clear();
                    }
                    LOG.info(
                        "Truncating all segments in {} because the first log index {} is greater than lastIndexKept={}",
                        this.segmentsPath, firstLogIndex, lastIndexKept);
                }

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
                    final byte[] data = getValueFromRocksDB(getKeyBytes(nextIndex));
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
                final byte[] keptData = getValueFromRocksDB(getKeyBytes(lastIndexKept));
                // The kept log's data is not stored in segments.
                if (!isMetadata(keptData)) {
                    //lastIndexKept's log is stored in rocksdb directly, try to find the first previous log that stored in segment.
                    long prevIndex = lastIndexKept - 1;
                    final long startIndex = keptFile.getFirstLogIndex();
                    while (prevIndex >= startIndex) {
                        final byte[] data = getValueFromRocksDB(getKeyBytes(prevIndex));
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
            this.writeLock.unlock();
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

    @Override
    protected WriteContext newWriteContext() {
        return new BarrierWriteContext();
    }

    @Override
    protected byte[] onDataAppend(final long logIndex, final byte[] value, final WriteContext ctx) throws IOException,
                                                                                                  InterruptedException {
        final int waitToWroteBytes = SegmentFile.getWriteBytes(value);
        SegmentFile lastSegmentFile = getLastSegmentFile(logIndex, waitToWroteBytes, true, ctx);
        if (lastSegmentFile.reachesFileEndBy(waitToWroteBytes)) {
            throw new IOException("Too large value size: " + value.length + ", maxSegmentFileSize="
                                  + this.maxSegmentFileSize);
        }
        if (value.length < this.valueSizeThreshold) {
            // Small value will be stored in rocksdb directly.
            lastSegmentFile.setLastLogIndex(logIndex);
            ctx.finishJob();
            return value;
        }
        // Large value is stored in segment file and returns an encoded location info that will be stored in rocksdb.
        final int pos = lastSegmentFile.write(logIndex, value, ctx);
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
        this.readLock.lock();
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
            this.readLock.unlock();
        }
    }

    private SegmentFile binarySearchFileByFirstLogIndex(final long logIndex) {
        this.readLock.lock();
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
            this.readLock.unlock();
        }
    }

    @Override
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
}
