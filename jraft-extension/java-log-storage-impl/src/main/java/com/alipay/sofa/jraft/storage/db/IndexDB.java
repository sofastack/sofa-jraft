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
package com.alipay.sofa.jraft.storage.db;

import java.util.List;

import com.alipay.sofa.jraft.storage.file.FileType;
import com.alipay.sofa.jraft.storage.file.index.IndexFile;
import com.alipay.sofa.jraft.storage.file.index.IndexFile.IndexEntry;
import com.alipay.sofa.jraft.storage.file.index.IndexType;
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
        final long lastIndex = getLastLogIndex();
        if (lastIndex != -1 && logIndex != getLastLogIndex() + 1) {
            return Pair.of(-1, (long) -1);
        }
        final int waitToWroteSize = IndexEntry.INDEX_SIZE;
        final IndexFile indexFile = (IndexFile) this.fileManager.getLastFile(logIndex, waitToWroteSize, true);
        if (indexFile != null) {
            final int pos = indexFile.appendIndex(logIndex, position, type.getType());
            final long expectFlushPosition = indexFile.getFileFromOffset() + pos + waitToWroteSize;
            return Pair.of(pos, expectFlushPosition);
        }
        return Pair.of(-1, (long) -1);
    }

    /**
     * Append IndexEntryArray(logIndex , position, indexType)
     * @return max expectFlushPosition
     */
    public Long appendBatchIndexAsync(final List<IndexEntry> indexArray) {
        long maxFlushPosition = -1;
        for (int i = 0; i < indexArray.size(); i++) {
            final IndexEntry index = indexArray.get(i);
            final long logIndex = index.getLogIndex();
            if (logIndex == getLastLogIndex() + 1) {
                final Pair<Integer, Long> flushPair = appendIndexAsync(logIndex, index.getPosition(),
                    index.getLogType() == 1 ? IndexType.IndexSegment : IndexType.IndexConf);
                maxFlushPosition = Math.max(maxFlushPosition, flushPair.getSecond());
            }
        }
        return maxFlushPosition;
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
        return Pair.of(firstSegmentPos, firstConfPos);
    }

    /**
     * Search the last segment log index and last conf log index from tail
     * @return pair of IndexEntry<last log index and position> (segment / conf)
     */
    public Pair<IndexEntry, IndexEntry> lookupLastLogIndexAndPosFromTail() {
        final long lastLogIndex = getLastLogIndex();
        final long firstLogIndex = getFirstLogIndex();
        IndexEntry lastSegmentIndex = null, lastConfIndex = null;
        long index = lastLogIndex;
        while (index >= firstLogIndex) {
            final IndexEntry indexEntry = lookupIndex(index);
            indexEntry.setLogIndex(index);
            if (indexEntry.getLogType() == IndexType.IndexSegment.getType() && lastSegmentIndex == null) {
                lastSegmentIndex = indexEntry;
            } else if (indexEntry.getLogType() == IndexType.IndexConf.getType() && lastConfIndex == null) {
                lastConfIndex = indexEntry;
            }
            if (lastSegmentIndex != null && lastConfIndex != null) {
                break;
            }
            index--;
        }
        return Pair.of(lastSegmentIndex, lastConfIndex);
    }

    @Override
    public FileType getDBFileType() {
        return FileType.FILE_INDEX;
    }

    @Override
    public int getDBFileSize() {
        return this.storeOptions.getIndexFileSize();
    }
}
