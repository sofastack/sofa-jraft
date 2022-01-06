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
package com.alipay.sofa.jraft.logStore.db;

import java.nio.file.Paths;
import java.util.List;

import com.alipay.sofa.common.profile.StringUtil;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.logStore.factory.LogStoreFactory;
import com.alipay.sofa.jraft.logStore.file.AbstractFile;
import com.alipay.sofa.jraft.logStore.file.AbstractFile.RecoverResult;
import com.alipay.sofa.jraft.logStore.file.FileManager;
import com.alipay.sofa.jraft.logStore.file.FileType;
import com.alipay.sofa.jraft.logStore.file.assit.AbortFile;
import com.alipay.sofa.jraft.logStore.file.assit.FlushStatusCheckpoint;
import com.alipay.sofa.jraft.logStore.file.segment.SegmentFile;
import com.alipay.sofa.jraft.logStore.service.FlushRequest;
import com.alipay.sofa.jraft.logStore.service.ServiceManager;
import com.alipay.sofa.jraft.option.StoreOptions;
import com.alipay.sofa.jraft.util.Pair;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DB parent class that invokes fileManager and anager
 * and wrappers uniform functions such as recover() etc..
 * @author hzh (642256541@qq.com)
 */
public abstract class AbstractDB implements Lifecycle<LogStoreFactory> {
    private static final Logger     LOG                     = LoggerFactory.getLogger(AbstractDB.class);
    private static final String     FLUSH_STATUS_CHECKPOINT = "FlushStatusCheckpoint";
    private static final String     ABORT_FILE              = "Abort";

    protected static final int      WAIT_FLUSH_TIMES        = 5;
    protected final String          storePath;
    protected FileManager           fileManager;
    protected ServiceManager        serviceManager;
    protected LogStoreFactory       logStoreFactory;
    protected StoreOptions          storeOptions;
    protected AbortFile             abortFile;
    protected FlushStatusCheckpoint flushStatusCheckpoint;

    protected AbstractDB(final String storePath) {
        this.storePath = storePath;
    }

    @Override
    public boolean init(final LogStoreFactory logStoreFactory) {
        this.logStoreFactory = logStoreFactory;
        this.storeOptions = logStoreFactory.getStoreOptions();
        final String flushStatusCheckpointPath = Paths.get(this.storePath, FLUSH_STATUS_CHECKPOINT).toString();
        final String abortFilePath = Paths.get(this.storePath, ABORT_FILE).toString();
        this.flushStatusCheckpoint = new FlushStatusCheckpoint(flushStatusCheckpointPath);
        this.abortFile = new AbortFile(abortFilePath);
        this.serviceManager = logStoreFactory.newServiceManager(this);
        if (!this.serviceManager.init(logStoreFactory)) {
            return false;
        }
        this.fileManager = logStoreFactory.newFileManager(getFileType(), this.storePath,
            this.serviceManager.getAllocateService());
        return true;
    }

    @Override
    public void shutdown() {
        if (this.serviceManager != null) {
            this.serviceManager.shutdown();
        }
        if (this.fileManager != null) {
            this.fileManager.shutdown();
        }
        if (this.abortFile != null) {
            this.abortFile.destroy();
        }
    }

    /**
     * @return this db's name
     */
    public String getDBName() {
        return getClass().getSimpleName();
    }

    /**
     * @return this db's file type (index or segmentLog or conf)
     */
    public abstract FileType getFileType();

    /**
     * @return this db's file size
     */
    public abstract int getFileSize();

    /**
     * Recover when startUp
     */
    public synchronized void recover() {
        final List<AbstractFile> files = this.fileManager.loadExistedFiles();
        try {
            if (files.isEmpty()) {
                this.fileManager.setFlushedPosition(0);
                this.abortFile.create();
                return;
            }
            this.flushStatusCheckpoint.load();
            final boolean normalExit = !this.abortFile.exists();
            long recoverOffset;
            int startRecoverIndex;
            if (!normalExit) {
                // Abnormal exit, should recover from lastCheckpointFile
                startRecoverIndex = findLastCheckpointFile(files, this.flushStatusCheckpoint);
                recoverOffset = (long) startRecoverIndex * (long) getFileSize();
                LOG.info("{} {} did not exit normally, will try to recover files from {}.", getDBName(),
                    this.storePath, startRecoverIndex);
            } else {
                // Normal exit , means all data have bean flushed, no need to recover
                startRecoverIndex = files.size();
                recoverOffset = this.flushStatusCheckpoint.flushPosition;
            }
            recoverOffset = recoverFiles(startRecoverIndex, files, recoverOffset, this.flushStatusCheckpoint);
            this.fileManager.setFlushedPosition(recoverOffset);

            if (normalExit) {
                this.abortFile.create();
            } else {
                this.abortFile.touch();
            }
        } catch (final Exception e) {
            LOG.error("Error on recover {} files , store path: {} , {}", getDBName(), this.storePath, e);
        } finally {
            startServiceManager();
        }
    }

    /**
     * Recover files
     * @return last recover offset
     */
    protected long recoverFiles(final int startRecoverIndex, final List<AbstractFile> files, long processOffset,
                                final FlushStatusCheckpoint checkPoint) {
        AbstractFile preFile = null;
        boolean needTruncate = false;
        for (int index = 0; index < files.size(); index++) {
            final AbstractFile file = files.get(index);
            final boolean isLastFile = index == files.size() - 1;

            if (index < startRecoverIndex) {
                // Update files' s position when don't need to recover
                file.updateAllPosition(getFileSize());
            } else {
                final RecoverResult result = file.recover();
                if (result.success()) {
                    if (result.recoverTotal()) {
                        processOffset += isLastFile ? result.getLastOffset() : getFileSize();
                    } else {
                        processOffset += result.getLastOffset();
                        needTruncate = true;
                    }
                } else {
                    needTruncate = true;
                }
            }

            if (preFile != null) {
                preFile.setLastLogIndex(file.getFirstLogIndex() - 1);
            }
            preFile = file;

            if (needTruncate) {
                // Error on recover files , truncate to processOffset
                this.fileManager.truncateSuffixByOffset(processOffset);
                break;
            }
        }
        if (preFile != null && preFile.getLastLogIndex() < 0) {
            preFile.setLastLogIndex(checkPoint.lastLogIndex);
        }
        return processOffset;
    }

    private int findLastCheckpointFile(final List<AbstractFile> files, final FlushStatusCheckpoint checkpoint) {
        if (checkpoint == null || checkpoint.fileName == null) {
            return 0;
        }
        for (int fileIndex = 0; fileIndex < files.size(); fileIndex++) {
            final AbstractFile file = files.get(fileIndex);
            if (StringUtil.equalsIgnoreCase(FilenameUtils.getName(file.getFilePath()), checkpoint.fileName)) {
                return fileIndex;
            }
        }
        return 0;
    }

    /**
     * Write the data and return it's wrote position.
     * @param data logEntry data
     * @return (wrotePosition, expectFlushPosition)
     */
    public Pair<Integer, Long> appendLogAsync(final long logIndex, final byte[] data) {
        final int waitToWroteSize = SegmentFile.getWriteBytes(data);
        final SegmentFile segmentFile = (SegmentFile) this.fileManager.getLastFile(logIndex, waitToWroteSize, true);
        final int pos = segmentFile.appendData(logIndex, data);
        final long expectFlushPosition = segmentFile.getFileFromOffset() + pos + waitToWroteSize;
        return new Pair<>(pos, expectFlushPosition);
    }

    public void registerFlushRequest(final FlushRequest flushRequest) throws InterruptedException {
        this.serviceManager.getFlushService().registerFlushRequest(flushRequest);
    }

    /**
     * Read log from the segmentFile.
     *
     * @param logIndex the log index
     * @param pos      the position to read
     * @return read data
     */
    public byte[] lookupLog(final long logIndex, final int pos) {
        final SegmentFile segmentFile = (SegmentFile) this.fileManager.findFileByLogIndex(logIndex, false);
        if (segmentFile != null) {
            final long targetFlushPosition = segmentFile.getFileFromOffset() + pos;
            if (targetFlushPosition <= getFlushedPosition()) {
                return segmentFile.lookupData(logIndex, pos);
            }
        }
        return null;
    }

    public void startServiceManager() {
        this.serviceManager.start();
    }

    public boolean flush(final int flushLeastPages) {
        return this.fileManager.flush(flushLeastPages);
    }

    public boolean truncatePrefix(final long firstIndexKept) {
        return this.fileManager.truncatePrefix(firstIndexKept);
    }

    public boolean truncateSuffix(final long lastIndexKept, final int pos) {
        return this.fileManager.truncateSuffix(lastIndexKept, pos);
    }

    public boolean reset(final long nextLogIndex) {
        this.flushStatusCheckpoint.destroy();
        this.fileManager.reset(nextLogIndex);
        return true;
    }

    public AbstractFile findStoreFileByOffset(final long offset) {
        return this.fileManager.findFileByOffset(offset, false);
    }

    public long getFlushedPosition() {
        return this.fileManager.getFlushedPosition();
    }

    public StoreOptions getStoreOptions() {
        return this.storeOptions;
    }

    public String getStorePath() {
        return this.storePath;
    }

    public long getFirstLogIndex() {
        return this.fileManager.getFirstLogIndex();
    }

    public long getLastLogIndex() {
        return this.fileManager.getLastLogIndex();
    }

}
