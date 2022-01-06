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
package com.alipay.sofa.jraft.logStore.service;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.logStore.db.AbstractDB;
import com.alipay.sofa.jraft.logStore.file.AbstractFile;
import com.alipay.sofa.jraft.logStore.file.assit.FlushStatusCheckpoint;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.ArrayBlockingQueue2;
import com.alipay.sofa.jraft.util.concurrent.BlockingQueue2;
import com.alipay.sofa.jraft.util.concurrent.ShutdownAbleThread;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timing flush service
 * @author hzh (642256541@qq.com)
 */
public class FlushService extends ShutdownAbleThread {
    private static final Logger                LOG                     = LoggerFactory.getLogger(FlushService.class);
    private static final int                   RETRY_TIMES             = 10;
    private static final int                   WAITING_TIME            = 100;
    private static final int                   QUEUE_SIZE              = 1024;
    private static final int                   CHECK_POINT_INTERVAL    = 1000 * 10;
    private static final String                FLUSH_STATUS_CHECKPOINT = "FlushStatusCheckpoint";
    private final FlushStatusCheckpoint        flushStatusCheckpoint;
    private final int                          flushLeastPages;
    private final AbstractDB                   abstractDB;
    private final BlockingQueue2<FlushRequest> requestQueue;
    private final List<FlushRequest>           tempQueue;
    private long                               lastCheckPointTs;

    public FlushService(final int flushLeastPages, final AbstractDB abstractDB) {
        this.flushLeastPages = flushLeastPages;
        this.abstractDB = abstractDB;
        final String checkPointPath = Paths.get(abstractDB.getStorePath(), FLUSH_STATUS_CHECKPOINT).toString();
        this.flushStatusCheckpoint = new FlushStatusCheckpoint(checkPointPath);
        this.requestQueue = new ArrayBlockingQueue2<>(QUEUE_SIZE);
        this.tempQueue = new ArrayList<>(QUEUE_SIZE);
    }

    public void registerFlushRequest(final FlushRequest request) throws InterruptedException {
        if (request != null) {
            this.requestQueue.offer(request, WAITING_TIME, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void run() {
        try {
            int size;
            while (!isStopped()) {
                // Block until there have available requests
                while ((size = this.requestQueue.blockingDrainTo(this.tempQueue, QUEUE_SIZE, WAITING_TIME,
                    TimeUnit.MILLISECONDS)) == 0) {
                    if (isStopped()) {
                        break;
                    }
                }
                if (size > 0) {
                    long maxPosition = 0;
                    for (int i = 0; i < size; i++) {
                        maxPosition = Math.max(maxPosition, this.tempQueue.get(i).getExpectedFlushPosition());
                    }
                    doFlush(maxPosition);
                    doWakeupConsumer();
                    if (System.currentTimeMillis() - this.lastCheckPointTs > CHECK_POINT_INTERVAL) {
                        Utils.runInThread(this::doCheckpoint);
                        this.lastCheckPointTs = System.currentTimeMillis();
                    }
                }
                this.tempQueue.clear();
            }
        } catch (final InterruptedException ignored) {
        }
        onShutdown();
    }

    private void doWakeupConsumer() {
        for (final FlushRequest flushRequest : this.tempQueue) {
            flushRequest.wakeup();
        }
    }

    private void doFlush(final long maxExpectedFlushPosition) {
        while (this.abstractDB.getFlushedPosition() < maxExpectedFlushPosition) {
            this.abstractDB.flush(this.flushLeastPages);
        }
    }

    private void doCheckpoint() {
        try {
            long flushedPosition = this.abstractDB.getFlushedPosition();
            if (flushedPosition % this.abstractDB.getFileSize() == 0) {
                flushedPosition -= 1;
            }
            final AbstractFile file = this.abstractDB.findStoreFileByOffset(flushedPosition);
            if (file != null) {
                this.flushStatusCheckpoint.setFileName(FilenameUtils.getName(file.getFilePath()));
                this.flushStatusCheckpoint.setFlushPosition(flushedPosition);
                this.flushStatusCheckpoint.setLastLogIndex(this.abstractDB.getLastLogIndex());
                this.flushStatusCheckpoint.save();
            }
        } catch (final IOException ioException) {
            LOG.error("Error on checkpoint flush status {}", this.flushStatusCheckpoint);
        }
    }

    @Override
    public void onShutdown() {
        // Try to flush RETRY_TIMES times when close, and do last checkpoint
        for (int i = 0; i < RETRY_TIMES; i++) {
            final boolean result = this.abstractDB.flush(0);
            if (!result) {
                break;
            }
        }
        doCheckpoint();
    }

}
