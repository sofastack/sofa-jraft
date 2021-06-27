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

import java.nio.ByteBuffer;

/**
 * SegmentLog's offset index file
 * @author hzh
 */
public class OffsetIndex extends AbstractIndex {

    private final Long       baseOffset;

    private Long             largestOffset;

    private final IndexEntry DEFAULT_ENTRY = new IndexEntry(-1, -1);

    public OffsetIndex(final String path, final Long baseOffset, final int maxSize) {
        super(path, maxSize);
        this.baseOffset = baseOffset;
        this.entrySize = 8;
        this.maxEntries = this.buffer.limit() / this.entrySize;
        this.entrySize = this.buffer.position() / this.entrySize;
    }

    /**
     * Append an offset index entry to this index file
     * @param offset log offset
     * @param position physical position
     */
    public void appendIndex(final Long offset, final int position) {
        Requires.requireTrue(!isFull(), "Exceeds the maximum index entry number of the index file : {}", this.path);
        Requires.requireTrue(offset > largestOffset, "The append offset {} is no larger than the last offset {}",
            offset, largestOffset);
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
     * @param  offset the target offset
     * @return a pair holding this offset and its physical file position.
     */
    public IndexEntry looUp(final Long offset) {
        this.readLock.lock();
        try {
            final ByteBuffer tempBuffer = this.buffer.duplicate();
            final int relativeOffset = relativeOffset(offset);
            final int slot = lowerBoundBinarySearch(tempBuffer, 0, this.entries - 1, relativeOffset);
            if (slot < 0) {
                return DEFAULT_ENTRY;
            } else {
                return parseEntry(tempBuffer, slot);
            }
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Parse an index entry from this index file
     */
    @Override
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
     * Truncates index to a known number of logIndex.
     */
    public void truncate(final Long logIndex) {
        this.writeLock.lock();
        try {
            final ByteBuffer tempBuffer = this.buffer.duplicate();
            final int offset = (int) (logIndex - this.baseOffset);
            final int slot = lowerBoundBinarySearch(tempBuffer, 0, this.entries - 1, offset);
            int newEntries = 0;
            if (slot < 0) {
                newEntries = 0;
            } else if (relativeOffset(tempBuffer, slot) == offset) {
                newEntries = slot;
            } else {
                newEntries = slot + 1;
            }
            truncateToEntries(newEntries);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void truncateToEntries(final int entries) {
        this.writeLock.lock();
        try {
            this.entries = entries;
            this.buffer.position(entries * entrySize);
            this.largestOffset = this.baseOffset + relativeOffset(this.buffer, entries - 1);
        } finally {
            this.writeLock.unlock();
        }
    }
}
