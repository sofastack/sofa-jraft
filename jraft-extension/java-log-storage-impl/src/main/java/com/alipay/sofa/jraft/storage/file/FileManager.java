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
package com.alipay.sofa.jraft.storage.file;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.factory.LogStoreFactory;
import com.alipay.sofa.jraft.storage.service.AllocateFileService;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Uses list to manage AbstractFile(Index,Segment) by logIndex and offset
 * @author hzh (642256541@qq.com)
 */
public class FileManager {

    private static final Logger       LOG       = LoggerFactory.getLogger(FileManager.class);
    private final String              storePath;

    private final FileType            fileType;

    private final StoreOptions        storeOptions;

    private final int                 fileSize;

    private final AllocateFileService allocateService;

    private final LogStoreFactory     logStoreFactory;

    private final List<AbstractFile>  files     = new ArrayList<>();

    private volatile long             flushedPosition;

    private final ReadWriteLock       lock      = new ReentrantReadWriteLock();
    private final Lock                readLock  = lock.readLock();
    private final Lock                writeLock = lock.writeLock();

    public FileManager(final FileType fileType, final int fileSize, final String storePath,
                       final LogStoreFactory logStoreFactory, final AllocateFileService allocateService) {
        this.storePath = storePath;
        this.fileType = fileType;
        this.allocateService = allocateService;
        this.storeOptions = logStoreFactory.getStoreOptions();
        this.logStoreFactory = logStoreFactory;
        this.fileSize = fileSize;
    }

    public static FileManagerBuilder newBuilder() {
        return new FileManagerBuilder();
    }

    public static class FileManagerBuilder {
        private FileType            fileType;

        private String              storePath;

        private Integer             fileSize;

        private LogStoreFactory     logStoreFactory;

        private AllocateFileService allocateService;

        public FileManagerBuilder storePath(final String storePath) {
            this.storePath = storePath;
            return this;
        }

        public FileManagerBuilder fileType(final FileType fileType) {
            this.fileType = fileType;
            return this;
        }

        public FileManagerBuilder logStoreFactory(final LogStoreFactory logStoreFactory) {
            this.logStoreFactory = logStoreFactory;
            return this;
        }

        public FileManagerBuilder fileSize(final Integer abstractFileSize) {
            this.fileSize = abstractFileSize;
            return this;
        }

        public FileManagerBuilder allocateService(final AllocateFileService allocateService) {
            this.allocateService = allocateService;
            return this;
        }

        public FileManager build() {
            Requires.requireNonNull(this.storePath, "storePath");
            Requires.requireNonNull(this.fileType, "fileType");
            Requires.requireNonNull(this.logStoreFactory, "logStoreFactory");
            Requires.requireNonNull(this.fileSize, "fileSize");
            Requires.requireNonNull(this.allocateService, "allocateService");
            return new FileManager(fileType, fileSize, storePath, logStoreFactory, allocateService);
        }
    }

    /**
     * Load abstract files that existed in this store path
     * This function will be called when db recover
     * @return the existed files list
     */
    public List<AbstractFile> loadExistedFiles() {
        final File dir = new File(this.storePath);
        if(!dir.exists()) {
            dir.mkdirs();
        }
        final File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return Collections.emptyList();
        }
        Arrays.sort(files, Comparator.comparing(this::getFileSequenceFromFileName));
        final List<AbstractFile> blankFiles = new ArrayList<>(files.length);
        long nextFileSequence = 0;
        for (final File file : files) {
            AbstractFile abstractFile;
            if ((abstractFile = checkFileCorrectnessAndMmap(file)) != null) {
                if (abstractFile.loadHeader() && !abstractFile.isBlank()) {
                    this.files.add(abstractFile);
                } else {
                    abstractFile.reset();
                    blankFiles.add(abstractFile);
                }
                nextFileSequence = Math.max(nextFileSequence, getFileSequenceFromFileName(file) + 1);
            }
        }
        this.allocateService.setNextFileSequence(nextFileSequence);
        // Add blank files to allocator so that we can reuse them
        this.allocateService.addBlankAbstractFiles(blankFiles);
        return this.files;
    }

    /**
     * Check file's correctness of name and length
     * @return mmap file
     */
    private AbstractFile checkFileCorrectnessAndMmap(final File file) {
        if (!file.exists() || !file.getName().endsWith(this.fileType.getFileSuffix()))
            return null;
        AbstractFile abstractFile = null;
        final long fileLength = file.length();
        if (fileLength == this.fileSize) {
            abstractFile = this.logStoreFactory.newFile(this.fileType, file.getPath());
        }
        return abstractFile;
    }

    public AbstractFile[] copyFiles() {
        this.readLock.lock();
        try {
            return this.files.toArray(new AbstractFile[] {});
        } finally {
            this.readLock.unlock();
        }
    }

    public AbstractFile getLastFile(final long logIndex, final int waitToWroteSize, final boolean createIfNecessary) {
        AbstractFile lastFile = null;
        while (true) {
            int fileCount = 0;
            this.readLock.lock();
            try {
                if (!this.files.isEmpty()) {
                    fileCount = this.files.size();
                    final AbstractFile file = this.files.get(fileCount - 1);
                    if (waitToWroteSize <= 0 || !file.reachesFileEndBy(waitToWroteSize)) {
                        lastFile = file;
                    } else if (file.reachesFileEndBy(waitToWroteSize)) {
                        // Reach file end , need to fill blank bytes in file end
                        file.fillEmptyBytesInFileEnd();
                    }
                }
            } finally {
                this.readLock.unlock();
            }
            // Try to get a new file
            if (lastFile == null && createIfNecessary) {
                this.writeLock.lock();
                try {
                    if (this.files.size() != fileCount) {
                        // That means already create a new file , just continue and try again
                        continue;
                    }
                    lastFile = this.allocateService.takeEmptyFile();
                    if (lastFile != null) {
                        final long newFileOffset = (long) this.files.size() * (long) this.fileSize;
                        lastFile.setFileFromOffset(newFileOffset);
                        this.files.add(lastFile);
                        this.swapOutFilesIfNecessary();
                        return lastFile;
                    } else {
                        continue;
                    }
                } catch (final Exception e) {
                    LOG.error("Error on create new abstract file , current logIndex:{}", logIndex);
                } finally {
                    this.writeLock.unlock();
                }
            }
            return lastFile;
        }
    }

    public void swapOutFilesIfNecessary() {
        this.readLock.lock();
        try {
            if (this.files.size() <= this.storeOptions.getKeepInMemoryFileCount()) {
                return;
            }
            int filesInMemoryCount = this.allocateService.getAllocatedFileCount();
            int swappedOutCount = 0;
            final int lastIndex = this.files.size() - 1;
            long lastSwappedOutPosition = 0;
            for (int i = lastIndex; i >= 0; i--) {
                final AbstractFile abstractFile = this.files.get(i);
                if (abstractFile.isMapped()) {
                    filesInMemoryCount++;
                    if (filesInMemoryCount >= this.storeOptions.getKeepInMemoryFileCount() && i != lastIndex) {
                        abstractFile.unmmap();
                        swappedOutCount++;
                        if (lastSwappedOutPosition == 0) {
                            lastSwappedOutPosition = abstractFile.getFileFromOffset() + abstractFile.getFileSize();
                        }
                    }
                }
            }
            // Because lastSwappedOutPosition means all the data had been flushed before lastSwappedOutPosition,
            // so we should update flush position
            if (getFlushedPosition() < lastSwappedOutPosition) {
                setFlushedPosition(lastSwappedOutPosition);
            }
            LOG.info("Swapped out {} abstract files", swappedOutCount);
        } catch (final Exception e) {
            LOG.error("Error on swap out files", e);
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * @return the file that contains this logIndex
     */
    public AbstractFile findFileByLogIndex(final long logIndex, final boolean returnFirstIfNotFound) {
        this.readLock.lock();
        try {
            if (this.files.isEmpty()) {
                return null;
            }
            if (this.files.size() == 1) {
                return getFirstFile();
            }
            int lo = 0, hi = this.files.size() - 1;
            while (lo <= hi) {
                final int mid = (lo + hi) >>> 1;
                final AbstractFile file = this.files.get(mid);
                if (file.getLastLogIndex() < logIndex) {
                    lo = mid + 1;
                } else if (file.getFirstLogIndex() > logIndex) {
                    hi = mid - 1;
                } else {
                    return this.files.get(mid);
                }
            }
            if (returnFirstIfNotFound) {
                return getFirstFile();
            }
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    /*
     * @param logIndex begin index
     * @return the files that contain of after this logIndex
     */
    public AbstractFile[] findFileFromLogIndex(final long logIndex) {
        this.readLock.lock();
        try {
            for (int i = 0; i < this.files.size(); i++) {
                AbstractFile file = this.files.get(i);
                if (file.getFirstLogIndex() <= logIndex && logIndex <= file.getLastLogIndex()) {
                    final AbstractFile[] result = new AbstractFile[this.files.size() - i + 1];
                    for (int j = i; j < this.files.size(); j++) {
                        result[j - i] = this.files.get(j);
                    }
                    return result;
                }
            }
        } finally {
            this.readLock.unlock();
        }
        return new AbstractFile[0];
    }

    /**
     * @return the file that contains this offset
     */
    public AbstractFile findFileByOffset(final long offset, final boolean returnFirstIfNotFound) {
        this.readLock.lock();
        try {
            if (this.files.size() == 0)
                return null;
            final AbstractFile firstAbstractFile = getFirstFile();
            final AbstractFile lastAbstractFile = getLastFile();
            if (firstAbstractFile != null && lastAbstractFile != null) {
                if (offset < firstAbstractFile.getFileFromOffset()
                    || offset >= lastAbstractFile.getFileFromOffset() + this.fileSize) {
                    LOG.warn(
                        "Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, fileSize: {}, fileNums: {}",
                        offset, firstAbstractFile.getFileFromOffset(), lastAbstractFile.getFileFromOffset()
                                                                       + this.fileSize, this.fileSize,
                        this.files.size());
                } else {
                    // Locate the index
                    final int index = (int) ((offset / this.fileSize) - (firstAbstractFile.getFileFromOffset() / this.fileSize));
                    AbstractFile targetFile;
                    targetFile = this.files.get(index);

                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.fileSize) {
                        return targetFile;
                    }

                    // If pre not found , then traverse to find target file
                    for (final AbstractFile abstractFile : this.files) {
                        if (offset >= abstractFile.getFileFromOffset()
                            && offset < abstractFile.getFileFromOffset() + this.fileSize) {
                            return abstractFile;
                        }
                    }
                }
                if (returnFirstIfNotFound) {
                    return firstAbstractFile;
                }
            }
        } catch (final Exception e) {
            LOG.error("Error on find abstractFile by offset :{}, file type:{}", offset, this.fileType.getFileName(), e);
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    /**
     * Flush from flushPosition
     * @return true if flush success
     */
    public boolean flush() {
        final long flushWhere = getFlushedPosition();
        final AbstractFile abstractFile = findFileByOffset(flushWhere, flushWhere == 0);
        if (abstractFile != null) {
            final int flushOffset = abstractFile.flush();
            setFlushedPosition(abstractFile.getFileFromOffset() + flushOffset);
            return getFlushedPosition() != flushWhere;
        }
        return false;
    }

    public AbstractFile getLastFile() {
        return this.files.get(this.files.size() - 1);
    }

    public AbstractFile getFirstFile() {
        if (!this.files.isEmpty()) {
            return this.files.get(0);
        }
        return null;
    }

    /**
     * Truncate files suffix by offset
     * Only be called when recover
     */
    public void truncateSuffixByOffset(final long offset) {
        this.readLock.lock();
        final ArrayList<AbstractFile> willRemoveFiles = new ArrayList<>();
        try {
            for (final AbstractFile file : this.files) {
                final long tailOffset = file.getFileFromOffset() + this.fileSize;
                if (tailOffset > offset) {
                    if (offset >= file.getFileFromOffset()) {
                        final int truncatePosition = (int) (offset % this.fileSize);
                        file.setWrotePosition(truncatePosition);
                        file.setFlushPosition(truncatePosition);
                    } else {
                        willRemoveFiles.add(file);
                    }
                }
            }
        } finally {
            this.readLock.unlock();
            for (final AbstractFile file : willRemoveFiles) {
                if (file != null) {
                    file.shutdown(1000, true);
                }
            }
            deleteFiles(willRemoveFiles);
        }
    }

    /**
     * If file's lastLogIndex < first_index_kept , it will be destroyed
     * But if file's firstLogIndex < firstIndexKept < file's  lastLogIndex, it will be retained
     */
    public boolean truncatePrefix(final long firstIndexKept) {
        this.readLock.lock();
        final List<AbstractFile> willRemoveFiles = new ArrayList<>();
        try {
            for (final AbstractFile abstractFile : this.files) {
                final long lastLogIndex = abstractFile.getLastLogIndex();
                if (lastLogIndex < firstIndexKept) {
                    willRemoveFiles.addAll(this.files);
                }
            }
            return true;
        } finally {
            this.readLock.unlock();
            for (final AbstractFile file : willRemoveFiles) {
                if (file != null) {
                    file.shutdown(1000, true);
                }
            }
            deleteFiles(willRemoveFiles);
        }
    }

    /**
     * Truncate files suffix by lastIndexKept , (last_index_kept, last_log_index]
     *  will be discarded.
     */
    public boolean truncateSuffix(final long lastIndexKept, final int pos) {
        if (getLastLogIndex() <= lastIndexKept) {
            return true;
        }
        this.readLock.lock();
        final List<AbstractFile> willRemoveFiles = new ArrayList<>();
        try {
            long retainPosition = 0;
            for (final AbstractFile abstractFile : this.files) {
                final long firstLogIndex = abstractFile.getFirstLogIndex();
                final long lastLogIndex = abstractFile.getLastLogIndex();
                if (lastLogIndex > lastIndexKept) {
                    if (lastIndexKept >= firstLogIndex) {
                        // Truncate
                        final int lastPosition = abstractFile.truncate(lastIndexKept + 1, pos);
                        retainPosition += lastPosition;
                    } else {
                        // Remove
                        willRemoveFiles.add(abstractFile);
                    }
                } else {
                    retainPosition += this.fileSize;
                }
            }
            // Update flush position
            if (retainPosition < getFlushedPosition())
                setFlushedPosition(retainPosition);
            return true;
        } finally {
            this.readLock.unlock();
            for (final AbstractFile file : willRemoveFiles) {
                if (file != null) {
                    file.shutdown(1000, true);
                }
            }
            deleteFiles(willRemoveFiles);
        }
    }

    /**
     * Clear all files
     */
    public boolean reset(final long nextLogIndex) {
        List<AbstractFile> destroyedFiles = new ArrayList<>();
        this.writeLock.lock();
        try {
            destroyedFiles.addAll(this.files);
            this.files.clear();
            setFlushedPosition(0);
            LOG.info("Destroyed all abstractFiles in path {} by resetting.", this.storePath);
        } finally {
            this.writeLock.unlock();
            for (final AbstractFile file : destroyedFiles) {
                file.shutdown(1000, true);
            }
        }
        return true;
    }

    private void deleteFiles(final List<AbstractFile> files) {
        this.writeLock.lock();
        try {
            if (!files.isEmpty()) {
                files.removeIf(file -> !this.files.contains(file));
                this.files.removeAll(files);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            if (!this.files.isEmpty()) {
                return this.files.get(0).getFirstLogIndex();
            }
            return -1;
        } finally {
            this.readLock.unlock();
        }
    }

    public long getLastLogIndex() {
        this.readLock.lock();
        try {
            if (!this.files.isEmpty()) {
                final AbstractFile lastFile = getLastFile();
                return lastFile.getLastLogIndex();
            }
            return -1;
        } finally {
            this.readLock.unlock();
        }
    }

    public void shutdown() {
        for (final AbstractFile file : this.files) {
            if (file != null) {
                file.shutdown(5000, false);
            }
        }
    }

    public long getFlushedPosition() {
        return flushedPosition;
    }

    public synchronized void setFlushedPosition(final long flushedPosition) {
        this.flushedPosition = flushedPosition;
    }

    public long getFileSequenceFromFileName(final File file) {
        final String name = file.getName();
        if (name.endsWith(this.fileType.getFileSuffix())) {
            int idx = name.indexOf(this.fileType.getFileSuffix());
            return Long.parseLong(name.substring(0, idx));
        }
        return 0;
    }
}
