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
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.LogConfigurationException;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.storage.log.CheckpointFile.Checkpoint;
import com.alipay.sofa.jraft.storage.log.SegmentFile.SegmentFileOptions;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Log Storage implementation based on rocksdb and segment files.
 * @author boyan(boyan@antfin.com)
 *
 */
public class RocksDBSegmentLogStorage extends RocksDBLogStorage {

    private static final int     DEFAULT_CHECKPOINT_INTERVAL_MS = 1000;

    private static final int     METADATA_SIZE                  = SegmentFile.MAGIC_BYTES_SIZE + 2 + 8 + 4;

    private static final int     MAX_SEGMENT_FILE_SIZE          = 1024 * 1024 * 1024;

    private static final Logger  LOG                            = LoggerFactory
                                                                    .getLogger(RocksDBSegmentLogStorage.class);

    //TODO use it
    private static int           DEFAULT_VALUE_SIZE_THRESHOLD   = 4096;

    private final int            valueSizeThreshold;
    private final String         segmentsPath;

    private final CheckpointFile checkpointFile;

    private List<SegmentFile>    segments;

    private final ReadWriteLock  readWriteLock                  = new ReentrantReadWriteLock(false);

    private final Lock           writeLock                      = this.readWriteLock.writeLock();
    private final Lock           readLock                       = this.readWriteLock.readLock();
    private volatile boolean     started                        = false;
    private Thread               checkpointThread;

    public RocksDBSegmentLogStorage(final String path, final RaftOptions raftOptions) {
        this(path, raftOptions, 0);
    }

    public RocksDBSegmentLogStorage(final String path, final RaftOptions raftOptions, final int valueSizeThreshold) {
        super(path, raftOptions);
        this.segmentsPath = path + File.separator + "segments";
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
            SegmentFile currLastFile = this.segments.get(this.segments.size() - 1);
            if (!currLastFile.isFull(waitToWroteSize)) {
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
                SegmentFile currLastFile = this.segments.get(this.segments.size() - 1);
                // Sync last file before creating new one.
                currLastFile.clearData(currLastFile.getWrotePos(), currLastFile.getSize());
                currLastFile.sync();
                currLastFile.setLastLogIndex(logIndex - 1);
            }
            SegmentFile segmentFile = new SegmentFile(logIndex, MAX_SEGMENT_FILE_SIZE, this.segmentsPath);
            LOG.info("Create new segment file:{} with firstLogIndex={}.", segmentFile.getPath(), logIndex);
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
        SegmentFile lastSegmentFile = getLastSegmentFileForRead();
        if (lastSegmentFile != null) {
            lastSegmentFile.sync();
        }
    }

    @Override
    protected boolean onInitLoaded() {
        long startMs = Utils.monotonicMs();
        this.writeLock.lock();
        try {
            File segmentsDir = new File(this.segmentsPath);
            try {
                FileUtils.forceMkdir(segmentsDir);
            } catch (IOException e) {
                LOG.error("Fail to create segments directory: {}", this.segmentsPath, e);
                return false;
            }
            Checkpoint checkpoint = null;
            try {
                checkpoint = this.checkpointFile.load();
                if (checkpoint != null) {
                    LOG.info("Loaded checkpoint: {} from {}.", checkpoint, this.checkpointFile.getPath());
                }
            } catch (IOException e) {
                LOG.error("Fail to load checkpoint file: {}", this.checkpointFile.getPath(), e);
                return false;
            }

            Pattern pattern = Pattern.compile("[0-9]+");

            File[] segmentFiles = segmentsDir.listFiles((final File dir, final String name) -> {
                return pattern.matcher(name).matches();
            });

            this.segments = new ArrayList<>();
            if (segmentFiles != null && segmentFiles.length > 0) {
                // Sort by file names.
                Arrays.sort(segmentFiles, (a, b) -> {
                    return Long.valueOf(a.getName()).compareTo(Long.valueOf(b.getName()));
                });

                String checkpointFileName = checkpoint != null ? String.valueOf(checkpoint.startOffset) : null;

                boolean recover = false;
                SegmentFile prevFile = null;
                for (int i = 0; i < segmentFiles.length; i++) {
                    File segFile = segmentFiles[i];
                    int pos = (int) segFile.length();
                    if (segFile.getName().equals(checkpointFileName)) {
                        recover = true;
                        pos = checkpoint.pos;
                    }
                    long firstLogIndex = Long.parseLong(segFile.getName());

                    SegmentFile segmentFile = new SegmentFile(firstLogIndex, MAX_SEGMENT_FILE_SIZE, this.segmentsPath);

                    if (!segmentFile.init(new SegmentFileOptions(recover, i == segmentFiles.length - 1, pos))) {
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

                if (prevFile != null && getLastLogIndex() > 0) {
                    prevFile.setLastLogIndex(getLastLogIndex());
                }

            } else {
                if (checkpoint != null) {
                    LOG.error("Missing segment files, checkpoint is:{}", checkpoint);
                    return false;
                }
            }

            LOG.info("{} Loaded {} segment files from path {}.", getServiceName(), this.segments.size(),
                this.segmentsPath);

            LOG.info("{} segments: \n{}", getServiceName(), descSegments());
            this.started = true;

            Thread cpThread = new Thread() {
                @Override
                public void run() {
                    while (RocksDBSegmentLogStorage.this.started) {
                        try {
                            doCheckpoint();
                            Thread.sleep(DEFAULT_CHECKPOINT_INTERVAL_MS);
                        } catch (InterruptedException e) {
                            continue;
                        }
                    }
                }
            };
            cpThread.setName(this.getClass().getSimpleName() + "-Checkpoint-Thread");
            cpThread.setDaemon(true);
            cpThread.start();
            this.checkpointThread = cpThread;
            return true;
        } catch (Exception e) {
            LOG.error("Fail to load segment files from path {}.", this.segmentsPath, e);
            return false;
        } finally {
            LOG.info("{} init and load cost {} ms.", getServiceName(), Utils.monotonicMs() - startMs);
            this.writeLock.unlock();
        }
    }

    private StringBuilder descSegments() {
        StringBuilder segmentsDesc = new StringBuilder("[\n");
        for (SegmentFile segFile : this.segments) {
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
        this.writeLock.lock();
        try {
            doCheckpoint();
            this.started = false;
            if (this.checkpointThread != null) {
                try {
                    this.checkpointThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            for (SegmentFile segmentFile : this.segments) {
                segmentFile.shutdown();
            }
            this.segments.clear();
        } finally {
            this.writeLock.unlock();
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
        } catch (IOException e) {
            LOG.error("Fail to do checkpoint, last segment file is {}.",
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
            for (SegmentFile segmentFile : this.segments) {
                segmentFile.destroy();
            }
            this.checkpointFile.destroy();
            this.segments.clear();
            LOG.info("Destroyed segments and checkpoint in path {}.", this.segmentsPath);
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
            int toIndex = binarySearchFileIndexByLogIndex(firstIndexKept);

            if (fromIndex < 0) {
                fromIndex = 0;
            }
            if (toIndex < 0) {
                return;
            }

            List<SegmentFile> removedFiles = this.segments.subList(fromIndex, toIndex);
            if (removedFiles != null) {
                for (SegmentFile segmentFile : removedFiles) {
                    segmentFile.destroy();
                }
                removedFiles.clear();
            }
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
            int fromIndex = binarySearchFileIndexByLogIndex(lastIndexKept);
            int toIndex = binarySearchFileIndexByLogIndex(getLastLogIndex());

            List<SegmentFile> removedFiles = this.segments.subList(fromIndex + 1, toIndex + 1);
            if (removedFiles != null) {
                for (SegmentFile segmentFile : removedFiles) {
                    segmentFile.destroy();
                }
                removedFiles.clear();
            }
            int keptIndex = binarySearchFileIndexByLogIndex(lastIndexKept);

            if (keptIndex >= 0) {
                SegmentFile keptFile = this.segments.get(keptIndex);
                int wrotePos = -1;

                // Try to find the right position to be truncated.
                {
                    // First, find in right [lastIndexKept + 1, getLastLogIndex()]
                    long nextIndex = lastIndexKept + 1;
                    long endIndex = Math.min(getLastLogIndex(), keptFile.getLastLogIndex());
                    while (nextIndex <= endIndex) {
                        byte[] data = getValueFromRocksDB(getKeyBytes(nextIndex));
                        if (data != null) {
                            if (data.length == METADATA_SIZE) {
                                if (!isMetadata(data)) {
                                    nextIndex++;
                                    continue;
                                }
                                wrotePos = getWrotePosition(data);
                                break;

                            } else {
                                nextIndex++;
                            }
                        } else {
                            break;
                        }
                    }
                }

                if (wrotePos < 0) {
                    // Second, try to find in left  [firstLogIndex, lastIndexKept) when lastIndexKept is not stored in segments.
                    byte[] keptData = getValueFromRocksDB(getKeyBytes(lastIndexKept));
                    if (!isMetadata(keptData)) {
                        long prevIndex = lastIndexKept - 1;
                        long startIndex = keptFile.getFirstLogIndex();
                        while (prevIndex >= startIndex) {
                            byte[] data = getValueFromRocksDB(getKeyBytes(prevIndex));
                            if (data != null) {
                                if (data.length == METADATA_SIZE) {
                                    if (!isMetadata(data)) {
                                        prevIndex--;
                                        continue;
                                    }
                                    wrotePos = getWrotePosition(data);
                                    break;
                                } else {
                                    prevIndex--;
                                }
                            } else {
                                throw new LogConfigurationException("Log entry data not found at index=" + prevIndex);
                            }
                        }
                    }
                }

                if (wrotePos >= 0) {
                    keptFile.truncateSuffix(wrotePos, lastIndexKept);
                }
                doCheckpoint();
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private int getWrotePosition(final byte[] data) {
        return Bits.getInt(data, SegmentFile.MAGIC_BYTES_SIZE + 2 + 8);
    }

    @Override
    protected byte[] onDataAppend(final long logIndex, final byte[] value) throws IOException {
        this.writeLock.lock();
        try {
            SegmentFile lastSegmentFile = getLastSegmentFile(logIndex, SegmentFile.writeSize(value), true);
            if (value.length < this.valueSizeThreshold) {
                lastSegmentFile.setLastLogIndex(logIndex);
                return value;
            }
            long startOffset = lastSegmentFile.getFirstLogIndex();
            int pos = lastSegmentFile.write(logIndex, value);
            byte[] newData = encodeMetaData(startOffset, pos);
            return newData;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Encode offset and position to a byte array in the format of:
     * <ul>
     *  <li> magic bytes(2B)</li>
     *  <li> reserved (2B)</li>
     *  <li> start offset(8B)</li>
     *  <li> wrote position(4B)</li>
     * </ul>
     * @param startOffset
     * @param pos
     * @return
     */
    private byte[] encodeMetaData(final long startOffset, final int pos) {
        byte[] newData = new byte[METADATA_SIZE];
        System.arraycopy(SegmentFile.MAGIC_BYTES, 0, newData, 0, SegmentFile.MAGIC_BYTES_SIZE);
        // 2 bytes reserved
        Bits.putLong(newData, SegmentFile.MAGIC_BYTES_SIZE + 2, startOffset);
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
                SegmentFile firstFile = this.segments.get(0);
                if (firstFile.contains(logIndex)) {
                    return 0;
                } else {
                    return -1;
                }
            }

            int low = 0;
            int high = this.segments.size() - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;

                SegmentFile file = this.segments.get(mid);
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
                SegmentFile firstFile = this.segments.get(0);
                if (firstFile.getFirstLogIndex() == logIndex) {
                    return firstFile;
                } else {
                    return null;
                }
            }

            int low = 0;
            int high = this.segments.size() - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;

                SegmentFile file = this.segments.get(mid);
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
        if (value == null || value.length != METADATA_SIZE) {
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

        long firstLogIndex = Bits.getLong(value, offset);
        int pos = Bits.getInt(value, offset + 8);
        SegmentFile file = binarySearchFileByFirstLogIndex(firstLogIndex);
        if (file == null) {
            return null;
        }
        return file.read(logIndex, pos);
    }

}
