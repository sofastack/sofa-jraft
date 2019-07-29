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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.error.LogEntryCorruptedException;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.storage.log.CheckpointFile.Checkpoint;
import com.alipay.sofa.jraft.storage.log.SegmentFile.SegmentFileOptions;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Log Storage implementation based on rocksdb and segment files.
 *
 * @author boyan(boyan@antfin.com)
 */
public class RocksDBSegmentLogStorage extends RocksDBLogStorage {

    private static final Logger      LOG                            = LoggerFactory
                                                                        .getLogger(RocksDBSegmentLogStorage.class);

    /**
     * Default checkpoint interval in milliseconds.
     */
    private static final int         DEFAULT_CHECKPOINT_INTERVAL_MS = SystemPropertyUtil
                                                                        .getInt(
                                                                            "jraft.log_storage.segment.checkpoint.interval.ms",
                                                                            5000);

    /**
     * Location metadata format:
     * 1. magic bytes
     * 2. reserved(2 B)
     * 3. segmentFileName(8 B)
     * 4. wrotePosition(4 B)
     */
    private static final int         LOCATION_METADATA_SIZE         = SegmentFile.MAGIC_BYTES_SIZE + 2 + 8 + 4;

    /**
     * Max segment file size, 1G
     */
    private static final int         MAX_SEGMENT_FILE_SIZE          = SystemPropertyUtil.getInt(
                                                                        "jraft.log_storage.segment.max.size.bytes",
                                                                        1024 * 1024 * 1024);

    // Default value size threshold to decide whether it will be stored in segments or rocksdb, default is 4K.
    // When the value size is less than 4K, it will be stored in rocksdb directly.
    private static int               DEFAULT_VALUE_SIZE_THRESHOLD   = SystemPropertyUtil
                                                                        .getInt(
                                                                            "jraft.log_storage.segment.value.threshold.bytes",
                                                                            4 * 1024);

    private final int                valueSizeThreshold;
    private final String             segmentsPath;
    private final CheckpointFile     checkpointFile;
    private List<SegmentFile>        segments;
    private final ReadWriteLock      readWriteLock                  = new ReentrantReadWriteLock();
    private final Lock               writeLock                      = this.readWriteLock.writeLock();
    private final Lock               readLock                       = this.readWriteLock.readLock();
    private ScheduledExecutorService checkpointExecutor;
    private final AbortFile          abortFile;

    public RocksDBSegmentLogStorage(final String path, final RaftOptions raftOptions) {
        this(path, raftOptions, DEFAULT_VALUE_SIZE_THRESHOLD);
    }

    public RocksDBSegmentLogStorage(final String path, final RaftOptions raftOptions, final int valueSizeThreshold) {
        super(path, raftOptions);
        this.segmentsPath = path + File.separator + "segments";
        this.abortFile = new AbortFile(this.segmentsPath + File.separator + "abort");
        this.checkpointFile = new CheckpointFile(this.segmentsPath + File.separator + "checkpoint");
        this.valueSizeThreshold = valueSizeThreshold;

    }

    private SegmentFile getLastSegmentFile(final long logIndex, final int waitToWroteSize,
                                           final boolean createIfNecessary) throws IOException {
        this.readLock.lock();
        try {
            return getLastSegmentFileUnLock(logIndex, waitToWroteSize, createIfNecessary);
        } finally {
            this.readLock.unlock();
        }
    }

    private SegmentFile getLastSegmentFileUnLock(final long logIndex, final int waitToWroteSize,
                                                 final boolean createIfNecessary) throws IOException {
        SegmentFile lastFile = null;
        if (!this.segments.isEmpty()) {
            final SegmentFile currLastFile = this.segments.get(this.segments.size() - 1);
            if (!currLastFile.reachesFileEndBy(waitToWroteSize)) {
                lastFile = currLastFile;
            }
        }

        if (lastFile == null && createIfNecessary) {
            lastFile = createNewSegmentFile(logIndex);
        }
        return lastFile;
    }

    private SegmentFile createNewSegmentFile(final long logIndex) throws IOException {
        this.writeLock.lock();
        try {
            if (!this.segments.isEmpty()) {
                // Sync current last file and correct it's lastLogIndex.
                final SegmentFile currLastFile = this.segments.get(this.segments.size() - 1);
                currLastFile.sync();
                currLastFile.setLastLogIndex(logIndex - 1);
            }
            final SegmentFile segmentFile = new SegmentFile(logIndex, MAX_SEGMENT_FILE_SIZE, this.segmentsPath);
            LOG.info("Create a new segment file {} with firstLogIndex={}.", segmentFile.getPath(), logIndex);
            if (!segmentFile.init(new SegmentFileOptions(false, true, 0))) {
                throw new IOException("Fail to create new segment file for logIndex: " + logIndex);
            }
            this.segments.add(segmentFile);
            return segmentFile;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    protected void onSync() throws IOException {
        final SegmentFile lastSegmentFile = getLastSegmentFileForRead();
        if (lastSegmentFile != null) {
            lastSegmentFile.sync();
        }
    }

    private static final Pattern SEGMENT_FILE_NAME_PATTERN = Pattern.compile("[0-9]+");

    @Override
    protected boolean onInitLoaded() {
        final long startMs = Utils.monotonicMs();
        this.writeLock.lock();
        try {
            final File segmentsDir = new File(this.segmentsPath);
            try {
                FileUtils.forceMkdir(segmentsDir);
            } catch (final IOException e) {
                LOG.error("Fail to create segments directory: {}", this.segmentsPath, e);
                return false;
            }
            final Checkpoint checkpoint;
            try {
                checkpoint = this.checkpointFile.load();
                if (checkpoint != null) {
                    LOG.info("Loaded checkpoint: {} from {}.", checkpoint, this.checkpointFile.getPath());
                }
            } catch (final IOException e) {
                LOG.error("Fail to load checkpoint file: {}", this.checkpointFile.getPath(), e);
                return false;
            }

            final File[] segmentFiles = segmentsDir
                    .listFiles((final File dir, final String name) -> SEGMENT_FILE_NAME_PATTERN.matcher(name).matches());

            final boolean normalExit = !this.abortFile.exists();
            if (!normalExit) {
                LOG.info("{} {} did not exit normally, will try to recover last file.", getServiceName(),
                    this.segmentsPath);
            }
            this.segments = new ArrayList<>(segmentFiles == null ? 10 : segmentFiles.length);
            if (segmentFiles != null && segmentFiles.length > 0) {
                // Sort by file names.
                Arrays.sort(segmentFiles, Comparator.comparing(a -> Long.valueOf(a.getName())));

                final String checkpointFileName = getCheckpointFileName(checkpoint);

                boolean needRecover = false;
                SegmentFile prevFile = null;
                for (int i = 0; i < segmentFiles.length; i++) {
                    final File segFile = segmentFiles[i];
                    final boolean isLastFile = i == segmentFiles.length - 1;
                    int pos = (int) segFile.length(); //position to recover or write.
                    if (segFile.getName().equals(checkpointFileName)) {
                        needRecover = true;
                        pos = checkpoint.committedPos;
                    } else {
                        if (needRecover) {
                            pos = 0;
                        }
                    }
                    final long firstLogIndex = Long.parseLong(segFile.getName());

                    final SegmentFile segmentFile = new SegmentFile(firstLogIndex, MAX_SEGMENT_FILE_SIZE,
                        this.segmentsPath);

                    if (!segmentFile.init(new SegmentFileOptions(needRecover && !normalExit, isLastFile, pos))) {
                        LOG.error("Fail to load segment file {}.", segmentFile.getPath());
                        segmentFile.shutdown();
                        return false;
                    }
                    this.segments.add(segmentFile);
                    if (prevFile != null) {
                        prevFile.setLastLogIndex(firstLogIndex - 1);
                    }
                    prevFile = segmentFile;
                }

                if (getLastLogIndex() > 0) {
                    prevFile.setLastLogIndex(getLastLogIndex());
                }

            } else {
                if (checkpoint != null) {
                    LOG.warn("Missing segment files, checkpoint is: {}", checkpoint);
                    return false;
                }
            }

            LOG.info("{} Loaded {} segment files from path {}.", getServiceName(), this.segments.size(),
                this.segmentsPath);

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
            return true;
        } catch (final Exception e) {
            LOG.error("Fail to load segment files from directory {}.", this.segmentsPath, e);
            return false;
        } finally {
            this.writeLock.unlock();
            LOG.info("{} init and load cost {} ms.", getServiceName(), Utils.monotonicMs() - startMs);
        }
    }

    private String getCheckpointFileName(final Checkpoint checkpoint) {
        return checkpoint != null ? SegmentFile.getSegmentFileName(checkpoint.firstLogIndex) : null;
    }

    private void startCheckpointTask() {
        this.checkpointExecutor = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory(getServiceName() + "-Checkpoint-Thread-", true));
        this.checkpointExecutor.scheduleAtFixedRate(this::doCheckpoint,
            DEFAULT_CHECKPOINT_INTERVAL_MS, DEFAULT_CHECKPOINT_INTERVAL_MS, TimeUnit.MILLISECONDS);
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

    @Override
    protected void onShutdown() {
        stopCheckpointTask();
        List<SegmentFile> shutdownFiles = Collections.emptyList();
        this.writeLock.lock();
        try {
            doCheckpoint();
            shutdownFiles = new ArrayList<>(shutdownFiles);
            this.segments.clear();
            if (!this.abortFile.destroy()) {
                LOG.error("Fail to delete abort file {}.", this.abortFile.getPath());
            }
        } finally {
            this.writeLock.unlock();
            for (final SegmentFile segmentFile : shutdownFiles) {
                segmentFile.shutdown();
            }
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
                this.checkpointFile.save(new Checkpoint(lastSegmentFile.getFirstLogIndex(), lastSegmentFile
                    .getCommittedPos()));
            }
        } catch (final IOException e) {
            LOG.error("Fatal error, fail to do checkpoint, last segment file is {}.",
                lastSegmentFile != null ? lastSegmentFile.getPath() : "null", e);
        }
    }

    private SegmentFile getLastSegmentFileForRead() throws IOException {
        return getLastSegmentFile(-1, 0, false);
    }

    @Override
    protected void onReset(final long nextLogIndex) {
        this.writeLock.lock();
        try {
            this.checkpointFile.destroy();
            for (final SegmentFile segmentFile : this.segments) {
                segmentFile.destroy();
            }
            this.segments.clear();
            LOG.info("Destroyed segments and checkpoint in path {} by resetting.", this.segmentsPath);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    protected void onTruncatePrefix(final long startIndex, final long firstIndexKept) throws RocksDBException,
                                                                                     IOException {
        this.writeLock.lock();
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
            for (final SegmentFile segmentFile : removedFiles) {
                segmentFile.destroy();
            }
            removedFiles.clear();
            doCheckpoint();
        } finally {
            this.writeLock.unlock();
        }
    }

    private boolean isMetadata(final byte[] data) {
        for (int offset = 0; offset < SegmentFile.MAGIC_BYTES_SIZE; offset++) {
            if (data[offset] != SegmentFile.MAGIC_BYTES[offset]) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void onTruncateSuffix(final long lastIndexKept) throws RocksDBException, IOException {
        this.writeLock.lock();
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
            for (final SegmentFile segmentFile : removedFiles) {
                segmentFile.destroy();
            }
            removedFiles.clear();

            // Process logs in keptFile(firstLogIndex=lastIndexKept)

            final SegmentFile keptFile = this.segments.get(keptFileIndex);
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
                            // Data not found, should not happen.
                            throw new LogEntryCorruptedException("Log entry data not found at index=" + prevIndex);
                        }
                    }
                }
            }

            if (logWrotePos >= 0 && logWrotePos < keptFile.getSize()) {
                // Truncate the file from wrotePos and set it's lastLogIndex=lastIndexKept.
                keptFile.truncateSuffix(logWrotePos, lastIndexKept);
            }
            // Finally, do checkpoint.
            doCheckpoint();

        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Retrieve the log wrote position from metadata.
     *
     * @param data the metadata
     * @return the log wrote position
     */
    private int getWrotePosition(final byte[] data) {
        return Bits.getInt(data, SegmentFile.MAGIC_BYTES_SIZE + 2 + 8);
    }

    @Override
    protected byte[] onDataAppend(final long logIndex, final byte[] value) throws IOException {
        this.writeLock.lock();
        try {
            final SegmentFile lastSegmentFile = getLastSegmentFile(logIndex, SegmentFile.getWriteBytes(value), true);
            if (value.length < this.valueSizeThreshold) {
                // Small value will be stored in rocksdb directly.
                lastSegmentFile.setLastLogIndex(logIndex);
                return value;
            }
            // Large value is stored in segment file and returns an encoded location info that will be stored in rocksdb.
            final long firstLogIndex = lastSegmentFile.getFirstLogIndex();
            final int pos = lastSegmentFile.write(logIndex, value);
            return encodeLocationMetadata(firstLogIndex, pos);
        } finally {
            this.writeLock.unlock();
        }
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
        System.arraycopy(SegmentFile.MAGIC_BYTES, 0, newData, 0, SegmentFile.MAGIC_BYTES_SIZE);
        // 2 bytes reserved
        Bits.putLong(newData, SegmentFile.MAGIC_BYTES_SIZE + 2, firstLogIndex);
        Bits.putInt(newData, SegmentFile.MAGIC_BYTES_SIZE + 2 + 8, pos);
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
        for (; offset < SegmentFile.MAGIC_BYTES_SIZE; offset++) {
            if (value[offset] != SegmentFile.MAGIC_BYTES[offset]) {
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
