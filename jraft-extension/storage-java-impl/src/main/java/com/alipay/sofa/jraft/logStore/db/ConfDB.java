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

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.logStore.file.FileHeader;
import com.alipay.sofa.jraft.logStore.file.FileType;
import com.alipay.sofa.jraft.logStore.file.segment.SegmentFile;

import java.util.Iterator;

/**
 * DB that stores configuration type log entry
 * @author hzh (642256541@qq.com)
 */
public class ConfDB extends AbstractDB {

    public ConfDB(final String storePath) {
        super(storePath);
    }

    public static class ConfIterator implements Iterator<LogEntry> {
        private final Object[]        abstractFiles;
        private int                   currentReadPos;
        private int                   currentFileId;
        private final LogEntryDecoder logEntryDecoder;

        public ConfIterator(final Object[] abstractFiles, final LogEntryDecoder logEntryDecoder) {
            this.abstractFiles = abstractFiles;
            this.logEntryDecoder = logEntryDecoder;
            if (abstractFiles.length > 0) {
                this.currentFileId = 0;
                this.currentReadPos = FileHeader.HEADER_SIZE;
            } else {
                this.currentFileId = -1;
            }
        }

        @Override
        public boolean hasNext() {
            return this.currentFileId >= 0 && this.currentFileId < this.abstractFiles.length;
        }

        @Override
        public LogEntry next() {
            if (this.currentFileId == -1)
                return null;
            byte[] data;
            while (true) {
                if (currentFileId >= this.abstractFiles.length)
                    return null;
                final SegmentFile segmentFile = (SegmentFile) this.abstractFiles[currentFileId];
                data = segmentFile.lookupData(this.currentReadPos);
                if (data == null) {
                    // Reach file end
                    this.currentFileId += 1;
                    this.currentReadPos = FileHeader.HEADER_SIZE;
                } else {
                    this.currentReadPos += SegmentFile.getWriteBytes(data);
                    return this.logEntryDecoder.decode(data);
                }
            }
        }
    }

    public ConfIterator Iterator(final LogEntryDecoder logEntryDecoder) {
        final Object[] files = this.fileManager.copyFiles();
        return new ConfIterator(files, logEntryDecoder);
    }

    @Override
    public FileType getFileType() {
        return FileType.FILE_CONFIGURATION;
    }

    @Override
    public int getFileSize() {
        return this.storeOptions.getConfFileSize();
    }
}
