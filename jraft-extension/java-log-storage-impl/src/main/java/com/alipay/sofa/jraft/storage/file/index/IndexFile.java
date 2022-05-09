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
package com.alipay.sofa.jraft.storage.file.index;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.storage.file.AbstractFile;

/**
 *  * File header:
 *  * <pre>
 *  *   magic bytes       first log index   file from offset       reserved
 *  *   [0x20 0x20]      [... 8 bytes...]   [... 8 bytes...]   [... 8 bytes...]
 *  * <pre>
 *  *
 *  * Every record format is:
 *  * <pre>
 *  *    Magic byte     index type     offset        position
 *  *   [0x57]         [1 byte]      [4 bytes]     [4 bytes]
 *  *</pre>
 *  *
 * The implementation of offset index
 * @author hzh (642256541@qq.com)
 */
public class IndexFile extends AbstractFile {

    private static final Logger LOG                     = LoggerFactory.getLogger(IndexFile.class);

    /**
     * Magic bytes for data buffer.
     */
    public static final byte[]  RECORD_MAGIC_BYTES      = new byte[] { (byte) 0x57 };

    public static final int     RECORD_MAGIC_BYTES_SIZE = RECORD_MAGIC_BYTES.length;

    public IndexFile(final String filePath, final int fileSize) {
        super(filePath, fileSize, true);
    }

    /**
     * The offset entry of Index, including (1 byte magic, 4 bytes offset, 4 bytes position, 1 byte logType)
     */
    public static class IndexEntry {
        // Log index
        private long            logIndex;
        // Relative offset
        private int             offset;
        // Physical position
        private int             position;
        // LogType
        private byte            logType;

        // Index entry size
        public static final int INDEX_SIZE = 10;

        public IndexEntry(final long logIndex, final int position, final byte logType) {
            this(logIndex, 0, position, logType);
        }

        public IndexEntry(final int offset, final int position) {
            this(0, offset, position, IndexType.IndexSegment.getType());
        }

        public IndexEntry(final long logIndex, final int offset, final int position, final byte logType) {
            this.logIndex = logIndex;
            this.offset = offset;
            this.position = position;
            this.logType = logType;
        }

        public static IndexEntry newInstance() {
            return new IndexEntry(-1, -1);
        }

        public void setLogIndex(final long logIndex) {
            this.logIndex = logIndex;
        }

        public long getLogIndex() {
            return logIndex;
        }

        public int getOffset() {
            return offset;
        }

        public int getPosition() {
            return position;
        }

        public byte getLogType() {
            return logType;
        }

        public boolean decode(final ByteBuffer buffer) {
            if (buffer == null || buffer.remaining() < INDEX_SIZE) {
                LOG.error("Fail to decode index entry , invalid buffer length: {}",
                    buffer == null ? 0 : buffer.remaining());
                return false;
            }
            final byte[] magic = new byte[1];
            buffer.get(magic);
            if (!Arrays.equals(magic, RECORD_MAGIC_BYTES)) {
                LOG.error("Fail to decode index entry, invalid buffer magic");
                return false;
            }
            this.logType = buffer.get();
            this.offset = buffer.getInt();
            this.position = buffer.getInt();
            return true;
        }

        @Override
        public String toString() {
            return "IndexEntry{" + "logIndex=" + logIndex + ", offset=" + offset + ", position=" + position
                   + ", logType=" + logType + '}';
        }
    }

    /**
     * Write the index entry
     *  @param logIndex the log index
     * @param position the physical position
     * @param logType [1] means logEntry , [2] means confEntry
     */
    public int appendIndex(final long logIndex, final int position, final byte logType) {
        this.writeLock.lock();
        try {
            assert (logIndex > getLastLogIndex());
            final byte[] writeData = encodeData(toRelativeOffset(logIndex), position, logType);
            return doAppend(logIndex, writeData);
        } finally {
            this.writeLock.unlock();
        }
    }

    private byte[] encodeData(final int offset, final int position, final byte logType) {
        final ByteBuffer buffer = ByteBuffer.allocate(getIndexSize());
        // Magics
        buffer.put(RECORD_MAGIC_BYTES);
        // logType (segmentLog or conf)
        buffer.put(logType);
        // offset from FirstLogIndex
        buffer.putInt(offset);
        // phyPosition in segmentFile
        buffer.putInt(position);
        buffer.flip();
        return buffer.array();
    }

    /**
     * Find the index entry
     * @param  logIndex the target log index
     * @return a pair holding this offset and its physical file position.
     */
    public IndexEntry lookupIndex(final long logIndex) {
        mapInIfNecessary();
        this.readLock.lock();
        try {
            final ByteBuffer byteBuffer = sliceByteBuffer();
            final int slot = (int) (logIndex - this.header.getFirstLogIndex());
            if (slot < 0) {
                return IndexEntry.newInstance();
            } else {
                return parseEntry(byteBuffer, slot);
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public CheckDataResult checkData(final ByteBuffer buffer) {
        if (buffer.remaining() < RECORD_MAGIC_BYTES_SIZE) {
            return CheckDataResult.CHECK_FAIL;
        }
        // Check magic
        final byte[] magic = new byte[RECORD_MAGIC_BYTES_SIZE];
        buffer.get(magic);
        if (!Arrays.equals(magic, RECORD_MAGIC_BYTES)) {
            return CheckDataResult.FILE_END;
        }
        // Check index type
        final byte indexType = buffer.get();
        if (indexType != IndexType.IndexSegment.getType() && indexType != IndexType.IndexConf.getType()) {
            return CheckDataResult.CHECK_FAIL;
        }
        if (buffer.remaining() < getIndexSize() - RECORD_MAGIC_BYTES_SIZE - 1) {
            return CheckDataResult.CHECK_FAIL;
        }
        final CheckDataResult result = CheckDataResult.CHECK_SUCCESS;
        result.setSize(getIndexSize());
        return result;
    }

    @Override
    public void onRecoverDone(final int recoverPosition) {
        final int indexNum = (recoverPosition - this.header.getHeaderSize()) / IndexEntry.INDEX_SIZE;
        this.header.setLastLogIndex(this.header.getFirstLogIndex() + indexNum);
    }

    @Override
    public int truncate(final long logIndex, final int pos) {
        this.writeLock.lock();
        try {
            if (logIndex < this.header.getFirstLogIndex() || logIndex > this.header.getLastLogIndex()) {
                return 0;
            }
            final int slot = (int) (logIndex - this.header.getFirstLogIndex());
            return truncateToSlot(slot);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Truncate to a known number of slot.
     */
    public int truncateToSlot(final int slot) {
        this.writeLock.lock();
        try {
            this.header.setLastLogIndex(this.header.getFirstLogIndex() + slot - 1);
            final int lastPos = this.header.getHeaderSize() + slot * getIndexSize();
            updateAllPosition(lastPos);
            clear(getWrotePosition());
            return lastPos;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Parse an index entry from this index file
     */
    private IndexEntry parseEntry(final ByteBuffer buffer, final int n) {
        final int pos = this.header.getHeaderSize() + n * getIndexSize();
        buffer.position(pos);
        final IndexEntry indexEntry = IndexEntry.newInstance();
        indexEntry.decode(buffer);
        return indexEntry;
    }

    /**
     * Return the relative offset
     */
    private int toRelativeOffset(final long offset) {
        if (this.header.isBlank()) {
            return 0;
        } else {
            return (int) (offset - this.header.getFirstLogIndex());
        }
    }

    /**
     * @return the IndexEntry's phyPosition in this IndexFile
     */
    public int calculateIndexPos(final long logIndex) {
        return (int) (this.header.getHeaderSize() + (logIndex - getFirstLogIndex() + 1) * getIndexSize());
    }

    public int getIndexSize() {
        return IndexEntry.INDEX_SIZE;
    }

}
