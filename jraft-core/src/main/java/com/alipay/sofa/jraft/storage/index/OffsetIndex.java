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
package com.alipay.sofa.jraft.storage.index;

import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SegmentLog's offset index file
 * @author hzh(642256541@qq.com)
 */
public class OffsetIndex {

    private static final Logger     LOG           = LoggerFactory.getLogger(OffsetIndex.class);

    private final int               HEADER_SIZE   = OffsetIndexHeader.getHeaderSize();

    private final OffsetIndexHeader header;

    // Size of one index entry
    private final int               entrySize     = 8;

    // File length
    private long                    length;

    // File path
    private final String            path;

    private final File              file;

    private final int               maxFileSize;

    // Mmap byte buffer.
    private MappedByteBuffer        buffer;

    // The number of index entries in this index file
    private volatile int            entries;

    // The maximum number of entries this index can hold
    private int                     maxEntries;

    // The largest log offset that this index file hold
    private long                    largestOffset;

    private final ReadWriteLock     readWriteLock = new ReentrantReadWriteLock(false);
    private final Lock              writeLock     = this.readWriteLock.writeLock();
    private final Lock              readLock      = this.readWriteLock.readLock();

    private final IndexEntry        EMPTY_ENTRY   = new IndexEntry(-1, -1);

    /**
     * The offset entry of Index
     */
    public static class IndexEntry {
        // Relative offset
        private final int offset;
        // Physical position
        private final int position;

        public IndexEntry(final int offset, final int position) {
            this.offset = offset;
            this.position = position;
        }

        public int getOffset() {
            return offset;
        }

        public int getPosition() {
            return position;
        }

    }

    public OffsetIndex(final String path, final int maxFileSize) {
        this.path = path;
        this.maxFileSize = maxFileSize;
        this.file = new File(path);
        this.header = new OffsetIndexHeader();
    }

    /**
     * Map index file to memory
     * @return
     */
    public boolean initAndLoad() {
        try {
            if (this.buffer != null) {
                return true;
            }
            final boolean newlyCreated = file.createNewFile();
            try (final RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
                if (newlyCreated) {
                    raf.setLength(this.HEADER_SIZE + maxFileSize);
                }
                this.length = raf.length();
                this.buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.length);
                if (newlyCreated) {
                    // If this file is not existed , save header
                    this.saveHeader(true);
                    changePositionWithHeaderSize(0);
                } else {
                    // If this file is existed , set position to last index entry and check header
                    final int lastEntryPosition = roundDownToExactMultiple(this.buffer.limit() - this.HEADER_SIZE,
                        this.entrySize);
                    changePositionWithHeaderSize(lastEntryPosition);
                    this.loadHeader();
                }
                LOG.info("Init an index file, entries: {} , position: {}", this.entries, this.buffer.position());
            }
        } catch (final Throwable t) {
            LOG.error("Fail to init index file {} , {}", this.path, t);
            return false;
        }
        changeMaxEntries();
        changeEntryNumber();
        changeLargestOffset();
        return true;
    }

    private void changeMaxEntries() {
        this.maxEntries = (this.buffer.limit() - this.HEADER_SIZE) / this.entrySize;
    }

    private void changeEntryNumber() {
        this.entries = (this.buffer.position() - this.HEADER_SIZE) / this.entrySize;
    }

    private void changeLargestOffset() {
        this.largestOffset = this.header.baseOffset + lastEntry().offset;
    }

    private void changePositionWithHeaderSize(final int pos) {
        this.buffer.position(this.HEADER_SIZE + pos);
    }

    /**
     * Todo: Optimize Performance
     * Append an offset index  to this index file
     * @param offset log offset
     * @param position physical position
     */
    public void appendIndex(final long offset, final int position) {
        Requires.requireTrue(!isFull(), "Exceeds the maximum index entry number of the index file : %s", this.path);
        Requires.requireTrue(offset > this.largestOffset, "The append offset %d is no larger than the last offset %d",
            offset, this.largestOffset);
        this.writeLock.lock();
        try {
            assert (checkPosition());
            final int wrotePos = this.HEADER_SIZE + this.entries * this.entrySize;
            this.buffer.position(wrotePos);
            // First time append , set base offset to header
            if (this.header.isBlank() || this.buffer.position() == this.HEADER_SIZE) {
                this.header.baseOffset = offset;
                saveHeader(false);
            }
            // Put relative offset
            final int relativeOffset = toRelativeOffset(offset);
            this.buffer.putInt(relativeOffset);
            // Put physical position
            this.buffer.putInt(position);
            this.entries += 1;
            this.largestOffset = offset;
            Requires.requireTrue(checkPosition(),
                "Incorrect index position of index file %s , the entries is %d , but the file position is %d ",
                this.path, this.entries, this.buffer.position());
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Find the largest offset less than or equal to the given targetOffset
     * @param  offset the target log offset
     * @return a pair holding this offset and its physical file position.
     */
    public IndexEntry looUp(final long offset) {
        this.readLock.lock();
        try {
            if (offset < this.header.baseOffset || offset > this.largestOffset) {
                return EMPTY_ENTRY;
            }
            // Duplicate() enables buffer's pointers are independent of each other
            final ByteBuffer tempBuffer = this.buffer.duplicate();
            final int relativeOffset = toRelativeOffset(offset);
            final int slot = binarySearchOffset(tempBuffer, 0, this.entries - 1, relativeOffset);
            if (slot < 0) {
                return EMPTY_ENTRY;
            } else {
                return parseEntry(tempBuffer, slot);
            }
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Truncate mmap to a known number of log offset.
     */
    public void truncate(final long offset) {
        this.writeLock.lock();
        try {
            if (offset < this.header.baseOffset) {
                return;
            }
            // Duplicate() enables buffer's pointers are independent of each other
            final ByteBuffer tempBuffer = this.buffer.duplicate();
            final int relativeOffset = toRelativeOffset(offset);
            final int slot = binarySearchOffset(tempBuffer, 0, this.entries - 1, relativeOffset);
            int newSlot = 0;
            // Find the correct slot
            if (slot < 0) {
                newSlot = 0;
            } else if (relativeOffset(tempBuffer, slot) == relativeOffset) {
                newSlot = slot;
            } else {
                newSlot = slot + 1;
            }
            truncateToEntries(newSlot);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Truncate mmap to a known number of slot.
     */
    public void truncateToEntries(final int slot) {
        this.writeLock.lock();
        try {
            this.entries = slot;
            changePositionWithHeaderSize(slot * this.entrySize);
            changeLargestOffset();
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Load header and check correctness
     */
    private boolean loadHeader() {
        final ByteBuffer tempBuffer = this.buffer.duplicate();
        tempBuffer.position(0);
        return this.header.decode(tempBuffer);
    }

    /**
     * Save header
     */
    private void saveHeader(final boolean sync) {
        int oldPos = this.buffer.position();
        try {
            this.buffer.position(0);
            final ByteBuffer headerBuf = this.header.encode();
            assert (headerBuf.remaining() == HEADER_SIZE);
            this.buffer.put(headerBuf);
            if (sync) {
                flush();
            }
        } finally {
            this.buffer.position(oldPos);
        }
    }

    /**
     * Only be called in segmentFile
     */
    public void swapIn() {
        this.writeLock.lock();
        try {
            this.initAndLoad();
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Flush data to disk
     */
    public void flush() {
        if (buffer != null) {
            buffer.force();
        }
    }

    /**
     * Reset index file
     */
    public void reset() {
        this.writeLock.lock();
        try {
            truncateToEntries(0);
            resize(this.maxFileSize);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Destroy index file.
     */
    public void destroy() {
        this.writeLock.lock();
        try {
            if (this.buffer == null) {
                return;
            }
            Utils.unmap(this.buffer);
            FileUtils.deleteQuietly(new File(this.path));
            LOG.info("Deleted offsetIndex file {}.", this.path);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Shutdown index file
     * Only be called in segmentFile
     */
    public void shutdown() {
        this.writeLock.lock();
        try {
            if (this.buffer == null) {
                return;
            }
            trimToValidSize();
            Utils.unmap(this.buffer);
            this.buffer = null;
            LOG.info("Unloaded offsetIndex file {}, current status: {}.", this.path, toString());
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Trim this index file to fit just the valid entries
     */
    private void trimToValidSize() {
        this.writeLock.lock();
        try {
            resize(this.entrySize * this.entries);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Resize file size
     * only be used in two ways:
     * 1.trimToValidSize()
     * 2.reset()
     */
    private boolean resize(final int newSize) {
        this.writeLock.lock();
        try {
            final int roundedNewSize = roundDownToExactMultiple(newSize, entrySize);
            if (this.length == roundedNewSize + this.HEADER_SIZE) {
                return false;
            } else {
                try (final RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
                    final int prePosition = this.buffer.position();
                    Utils.unmap(this.buffer);
                    raf.setLength(this.HEADER_SIZE + roundedNewSize);
                    this.length = raf.length();
                    this.buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.length);
                    this.buffer.position(prePosition);
                    changeMaxEntries();
                    LOG.info("Resize an index file, entries: {}", this.entries);
                } catch (final Throwable t) {
                    LOG.error("Fail to resize index file {}.", this.path, t);
                }
                return true;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * The binary search algorithm is used to find slot for the given target offset.
     */
    private int binarySearchOffset(final ByteBuffer buffer, final int begin, final int end, final int target) {
        int lo = begin;
        int hi = end;
        while (lo <= hi) {
            final int mid = lo + (hi - lo) / 2;
            final IndexEntry entry = parseEntry(buffer, mid);
            final int offset = entry.getOffset();
            if (target == offset) {
                return mid;
            } else if (target < offset) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return -1;
    }

    /**
     * Parse an index entry from this index file
     */
    private IndexEntry parseEntry(final ByteBuffer buffer, final int n) {
        return new IndexEntry(relativeOffset(buffer, n), physical(buffer, n));
    }

    /**
     * Return the relative offset
     */
    private int toRelativeOffset(final long offset) {
        return (int) (offset - this.header.baseOffset);
    }

    /**
     * Return the relative offset of index entry n
     */
    private int relativeOffset(final ByteBuffer buffer, final int n) {
        return buffer.getInt(this.HEADER_SIZE + n * entrySize);
    }

    /**
     * Return the physical position of index entry n
     */
    private int physical(final ByteBuffer buffer, final int n) {
        return buffer.getInt(this.HEADER_SIZE + n * entrySize + 4);
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     */
    private int roundDownToExactMultiple(final int number, final int factor) {
        return factor * (number / factor);
    }

    /**
     * Return last entry in this index file
     */
    private IndexEntry lastEntry() {
        if (this.entries >= 1) {
            return parseEntry(this.buffer, this.entries - 1);
        } else {
            return EMPTY_ENTRY;
        }
    }

    public boolean isFull() {
        return entries >= maxEntries;
    }

    private boolean checkPosition() {
        return (HEADER_SIZE + entries * entrySize) == buffer.position();
    }

    public void setBaseOffset(final long baseOffset) {
        this.header.baseOffset = baseOffset;
    }

    public long getLargestOffset() {
        return largestOffset;
    }

    public int getEntries() {
        return entries;
    }

    @OnlyForTest
    public int getPosition() {
        return this.buffer.position();
    }

    @Override
    public String toString() {
        return "OffsetIndex{" + "entrySize=" + entrySize + ", length=" + length + ", entries=" + entries
               + ", maxEntries=" + maxEntries + ", baseOffset=" + this.header.baseOffset + '}';
    }
}
