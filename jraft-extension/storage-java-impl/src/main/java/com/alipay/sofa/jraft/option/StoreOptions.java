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
package com.alipay.sofa.jraft.option;

import com.alipay.sofa.jraft.logStore.file.FileHeader;
import com.alipay.sofa.jraft.logStore.file.index.IndexFile.IndexEntry;

/**
 * Storage options
 * @author hzh (642256541@qq.com)
 */
public class StoreOptions {

    private String  storagePath           = "localLog";

    // Default is 0.5G
    private int     segmentFileSize       = 1024 * 1024 * 512;

    private int     indexFileSize         = FileHeader.HEADER_SIZE + 5000000 * IndexEntry.INDEX_SIZE;

    private int     confFileSize          = 1024 * 1024 * 512;

    // Whether enable warm up file when pre allocate
    private boolean enableWarmUpFile      = true;

    // Pre allocate files
    private int     preAllocateFileCount  = 2;

    // How many files can be kept in memory
    private int     keepInMemoryFileCount = 3;

    // Time limit when recover db
    private int     recoverDBTimeout      = 1000 * 60 * 10;

    // Time limit when shutdown db
    private int     shutdownDBTimeout     = 1000 * 60 * 10;

    // Time limit when waiting flush to expected position
    private int     waitingFlushTimeout   = 30 * 1000;

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(final String storagePath) {
        this.storagePath = storagePath;
    }

    public boolean isEnableWarmUpFile() {
        return enableWarmUpFile;
    }

    public void setEnableWarmUpFile(final boolean enableWarmUpFile) {
        this.enableWarmUpFile = enableWarmUpFile;
    }

    public int getSegmentFileSize() {
        return segmentFileSize;
    }

    public void setSegmentFileSize(final int segmentFileSize) {
        this.segmentFileSize = segmentFileSize;
    }

    public int getIndexFileSize() {
        return indexFileSize;
    }

    public void setIndexFileSize(final int indexFileSize) {
        this.indexFileSize = indexFileSize;
    }

    public int getConfFileSize() {
        return confFileSize;
    }

    public void setConfFileSize(final int confFileSize) {
        this.confFileSize = confFileSize;
    }

    public int getPreAllocateFileCount() {
        return preAllocateFileCount;
    }

    public void setPreAllocateFileCount(final int preAllocateFileCount) {
        this.preAllocateFileCount = preAllocateFileCount;
    }

    public int getKeepInMemoryFileCount() {
        return keepInMemoryFileCount;
    }

    public void setKeepInMemoryFileCount(final int keepInMemoryFileCount) {
        this.keepInMemoryFileCount = keepInMemoryFileCount;
    }

    public void setRecoverDBTimeout(final int recoverDBTimeout) {
        this.recoverDBTimeout = recoverDBTimeout;
    }

    public int getRecoverDBTimeout() {
        return recoverDBTimeout;
    }

    public void setShutdownDBTimeout(final int shutdownDBTimeout) {
        this.shutdownDBTimeout = shutdownDBTimeout;
    }

    public int getShutdownDBTimeout() {
        return shutdownDBTimeout;
    }

    public void setWaitingFlushTimeout(final int waitingFlushTimeout) {
        this.waitingFlushTimeout = waitingFlushTimeout;
    }

    public int getWaitingFlushTimeout() {
        return waitingFlushTimeout;
    }
}
