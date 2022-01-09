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

import com.alipay.sofa.jraft.logStore.file.FileType;
import com.alipay.sofa.jraft.logStore.file.index.IndexFile;
import com.alipay.sofa.jraft.logStore.file.index.IndexFile.IndexEntry;
import com.alipay.sofa.jraft.logStore.file.index.IndexType;
import com.alipay.sofa.jraft.util.Pair;

/**
 * DB that stores index entry
 * @author hzh (642256541@qq.com)
 */
public class IndexDB extends AbstractDB {

    public IndexDB(final String storePath) {
        super(storePath);
    }

    /**
     * Append IndexEntry(logIndex , position, indexType)
     * @return (wrotePosition, expectFlushPosition)
     */
    public Pair<Integer, Long> appendIndexAsync(final long logIndex, final int position, final IndexType type) {
        final int waitToWroteSize = IndexEntry.INDEX_SIZE;
        final IndexFile indexFile = (IndexFile) this.fileManager.getLastFile(logIndex, waitToWroteSize, true);
        if (indexFile != null) {
            final int pos = indexFile.appendIndex(logIndex, position, type.getType());
            final long expectFlushPosition = indexFile.getFileFromOffset() + pos + waitToWroteSize;
            return new Pair<>(pos, expectFlushPosition);
        }
        return new Pair(-1, -1);
    }

    /**
     * Lookup IndexEntry by logIndex
     */
    public IndexEntry lookupIndex(final long logIndex) {
        final IndexFile indexFile = (IndexFile) this.fileManager.findFileByLogIndex(logIndex, false);
        if (indexFile != null) {
            final int indexPos = indexFile.calculateIndexPos(logIndex);
            final long targetFlushPosition = indexFile.getFileFromOffset() + indexPos;
            if (targetFlushPosition <= getFlushedPosition()) {
                return indexFile.lookupIndex(logIndex);
            }
        }
        return IndexEntry.newInstance();
    }

    /**
     * Search the first segment log index and first conf log index from logIndex
     * @return pair of first log pos(segment / conf)
     */
    public Pair<Integer, Integer> lookupFirstLogPosFromLogIndex(final long logIndex) {

        final long lastLogIndex = getLastLogIndex();
        int firstSegmentPos = -1;
        int firstConfPos = -1;
        long index = logIndex;
        while (index <= lastLogIndex) {
            final IndexEntry indexEntry = lookupIndex(index);
            if (indexEntry.getLogType() == IndexType.IndexSegment.getType() && firstSegmentPos == -1) {
                firstSegmentPos = indexEntry.getPosition();
            } else if (indexEntry.getLogType() == IndexType.IndexConf.getType() && firstConfPos == -1) {
                firstConfPos = indexEntry.getPosition();
            }
            if (firstSegmentPos > 0 && firstConfPos > 0) {
                break;
            }
            index++;
        }
        return new Pair<>(firstSegmentPos, firstConfPos);
    }

    @Override
    public FileType getFileType() {
        return FileType.FILE_INDEX;
    }

    @Override
    public int getFileSize() {
        return this.storeOptions.getIndexFileSize();
    }
}
