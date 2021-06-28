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

import com.alipay.sofa.jraft.util.Requires;
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
 * @author hzh
 */
public class OffsetIndex {

    private final Logger        LOG           = LoggerFactory.getLogger(OffsetIndex.class);

    // Size of one index entry
    private final int           entrySize     = 8;

    // File length
    private Long                length;

    // File path
    private final String        path;

    private final File          file;

    // mmap byte buffer.
    private MappedByteBuffer    buffer;

    // The number of index entries in this index file
    private volatile int        entries;

    // The maximum number of entries this index can hold
    private int                 maxEntries;

    // Base offset of this index file
    private final Long          baseOffset;

    // The largest log offset that this index file hold
    private Long                largestOffset;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);
    private final Lock          writeLock     = this.readWriteLock.writeLock();
    private final Lock          readLock      = this.readWriteLock.readLock();

    private final IndexEntry    EMPTY_ENTRY   = new IndexEntry(-1, -1);

    public OffsetIndex(final String path, final Long baseOffset, final int maxSize) {
        this.path = path;
        this.file = new File(path);
        // Init mmap buffer
        try {
            final boolean newlyCreated = file.createNewFile();
            try (final RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
                if (newlyCreated) {
                    raf.setLength(maxSize);
                }
                this.length = raf.length();
                this.buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.length);
                if (newlyCreated) {
                    this.buffer.position(0);
                } else {
                    // If this file is existed , set position to last index entry
                    this.buffer.position(roundDownToExactMultiple(this.buffer.limit(), this.entrySize));
                }
                LOG.info("init a index file, entries: {}", this.entries);
            }
        } catch (final Throwable t) {
            LOG.error("Fail to init index file {}.", this.path, t);
        }
        this.baseOffset = baseOffset;
        this.maxEntries = this.buffer.limit() / this.entrySize;
        this.entries = this.buffer.position() / this.entrySize;
    }

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

    /**
     * Append an offset index  to this index file
     * @param offset log offset
     * @param position physical position
     */
    public void appendIndex(final Long offset, final int position) {
        Requires.requireTrue(!isFull(), "Exceeds the maximum index entry number of the index file : {}", this.path);
        Requires.requireTrue(offset > this.largestOffset, "The append offset {} is no larger than the last offset {}",
            offset, this.largestOffset);
        this.writeLock.lock();
        try {
            // put relative offset
            final int relativeOffset = relativeOffset(offset);
            this.buffer.putInt(relativeOffset);
            // put physical position
            this.buffer.putInt(position);
            this.entries += 1;
            this.largestOffset = offset;
            Requires.requireTrue(checkPosition(),
                "Incorrect index position of index file {} , the entries is {} , but the file position is {} ",
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
    public IndexEntry looUp(final Long offset) {
        this.readLock.lock();
        try {
            // Duplicate() enables buffer's pointers are independent of each other
            final ByteBuffer tempBuffer = this.buffer.duplicate();
            final int relativeOffset = relativeOffset(offset);
            final int slot = lowerBoundBinarySearch(tempBuffer, 0, this.entries - 1, relativeOffset);
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
    public void truncate(final Long offset) {
        this.writeLock.lock();
        try {
            // Duplicate() enables buffer's pointers are independent of each other
            final ByteBuffer tempBuffer = this.buffer.duplicate();
            final int relativeOffset = relativeOffset(offset);
            final int slot = lowerBoundBinarySearch(tempBuffer, 0, this.entries - 1, relativeOffset);
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
    private void truncateToEntries(final int slot) {
        this.writeLock.lock();
        try {
            this.entries = slot;
            this.buffer.position(slot * entrySize);
            this.largestOffset = this.baseOffset + relativeOffset(this.buffer, slot - 1);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * The binary search algorithm is used to find the lower bound for the given target.
     */
    public int lowerBoundBinarySearch(final ByteBuffer buffer, final int begin, final int end, final int target) {
        int lo = begin;
        int hi = end;
        while (lo < hi) {
            final int mid = (lo + hi + 1) / 2;
            final IndexEntry entry = parseEntry(buffer, mid);
            if (target < entry.getOffset()) {
                hi = mid - 1;
            } else if (target >= entry.getOffset()) {
                lo = mid;
            }
        }
        return lo;
    }

    /**
     * Flush data to disk
     */
    public void flush() {
        this.writeLock.lock();
        try {
            buffer.force();
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Parse an index entry from this index file
     */
    public IndexEntry parseEntry(final ByteBuffer buffer, final int n) {
        return new IndexEntry(relativeOffset(buffer, n), physical(buffer, n));
    }

    /**
     * Return the relative offset
     */
    public int relativeOffset(final Long offset) {
        return (int) (offset - this.baseOffset);
    }

    /**
     * Return the relative offset of index entry n
     */
    public int relativeOffset(final ByteBuffer buffer, final int n) {
        return buffer.getInt(n * entrySize);
    }

    /**
     * Return the physical position of index entry n
     */
    public int physical(final ByteBuffer buffer, final int n) {
        return buffer.getInt(n * entrySize + 4);
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     */
    public int roundDownToExactMultiple(final int number, final int factor) {
        return factor * (number / factor);
    }

    public boolean isFull() {
        return entries >= maxEntries;
    }

    public boolean checkPosition() {
        return entries * entrySize == buffer.position();
    }
}
