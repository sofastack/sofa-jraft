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
package com.alipay.sofa.jraft.storage.service;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.storage.db.AbstractDB;
import com.alipay.sofa.jraft.storage.factory.LogStoreFactory;
import com.alipay.sofa.jraft.storage.file.AbstractFile;
import com.alipay.sofa.jraft.storage.file.FileType;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.concurrent.ShutdownAbleThread;

/**
 * Pre allocate abstractFile service
 * @author boyan(boyan@antfin.com), hzh (642256541@qq.com)
 */
public class AllocateFileService extends ShutdownAbleThread {
    private final FileType                    fileType;
    private final String                      storePath;
    private final StoreOptions                storeOptions;
    private final LogStoreFactory             logStoreFactory;
    // Pre-allocated files
    private final ArrayDeque<AllocatedResult> blankFiles       = new ArrayDeque<>();
    // Abstract file sequence.
    private final AtomicLong                  nextFileSequence = new AtomicLong(0);
    private final Lock                        allocateLock     = new ReentrantLock();
    private final Condition                   fullCond         = this.allocateLock.newCondition();
    private final Condition                   emptyCond        = this.allocateLock.newCondition();

    public AllocateFileService(final AbstractDB abstractDB, final LogStoreFactory logStoreFactory) {
        this.fileType = abstractDB.getDBFileType();
        this.storePath = abstractDB.getStorePath();
        this.storeOptions = logStoreFactory.getStoreOptions();
        this.logStoreFactory = logStoreFactory;
    }

    @OnlyForTest
    public AllocateFileService(final FileType fileType, final String storePath, final LogStoreFactory logStoreFactory) {
        this.fileType = fileType;
        this.storePath = storePath;
        this.logStoreFactory = logStoreFactory;
        this.storeOptions = logStoreFactory.getStoreOptions();
    }

    public static class AllocatedResult {
        AbstractFile abstractFile;

        public AllocatedResult(final AbstractFile abstractFile) {
            super();
            this.abstractFile = abstractFile;
        }
    }

    @Override
    public void run() {
        try {
            while (!isStopped()) {
                doAllocateFileInLock();
            }
        } catch (final InterruptedException ignored) {
        }
        onShutdown();
    }

    @Override
    public void onShutdown() {
        // Destroy all empty file
        for (final AllocatedResult result : this.blankFiles) {
            if (result.abstractFile != null) {
                result.abstractFile.shutdown(5000, false);
            }
        }
    }

    private AbstractFile allocateNewAbstractFile() {
        final String newFilePath = getNewFilePath();
        final AbstractFile file = this.logStoreFactory.newFile(this.fileType, newFilePath);
        if (this.storeOptions.isEnableWarmUpFile()) {
            file.warmupFile();
        }
        return file;
    }

    private void doAllocateFile0() {
        final AbstractFile abstractFile = allocateNewAbstractFile();
        this.blankFiles.add(new AllocatedResult(abstractFile));
    }

    private void doAllocateFileInLock() throws InterruptedException {
        this.allocateLock.lock();
        try {
            while (this.blankFiles.size() >= this.storeOptions.getPreAllocateFileCount()) {
                this.fullCond.await();
            }
            doAllocateFile0();
            this.emptyCond.signal();
        } finally {
            this.allocateLock.unlock();
        }
    }

    public AbstractFile takeEmptyFile() throws Exception {
        this.allocateLock.lock();
        try {
            while (this.blankFiles.isEmpty()) {
                this.emptyCond.await();
            }
            final AllocatedResult result = this.blankFiles.pollFirst();
            this.fullCond.signal();
            return result.abstractFile;
        } finally {
            this.allocateLock.unlock();
        }
    }

    public int getAllocatedFileCount() {
        return this.blankFiles.size();
    }

    private String getNewFilePath() {
        return this.storePath + File.separator + String.format("%019d", this.nextFileSequence.getAndIncrement())
               + this.fileType.getFileSuffix();
    }

    public void setNextFileSequence(final long sequence) {
        this.nextFileSequence.set(sequence);
    }

    public void addBlankAbstractFiles(final List<AbstractFile> blankFiles) {
        for (final AbstractFile blankFile : blankFiles) {
            this.blankFiles.add(new AllocatedResult(blankFile));
        }
    }

}
