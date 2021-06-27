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

import com.alipay.sofa.jraft.util.MmapUtil;
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
 * An abstract index class that defines functions related to mmap and search
 * @author hzh
 */
public abstract class AbstractIndex {

    private Logger              LOG           = LoggerFactory.getLogger(AbstractIndex.class);

    // Length of one index
    private int                 entrySize     = 8;

    // File size
    private Long                length;

    // File path
    private final String        path;

    private final File          file;

    // mmap byte buffer.
    private MappedByteBuffer    buffer;

    // The number of entries in this index
    private volatile int        entries;

    // The maximum number of entries this index can hold
    private int                 maxEntries;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);
    private final Lock          writeLock     = this.readWriteLock.writeLock();
    private final Lock          readLock      = this.readWriteLock.readLock();

    public AbstractIndex(final String path, final int maxSize) {
        this.path = path;
        this.file = new File(path);
        try {
            boolean newlyCreated = file.createNewFile();
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
                this.maxEntries = this.buffer.limit() / this.entrySize;
                this.entrySize = this.buffer.position() / this.entrySize;
            }
        } catch (final Throwable t) {
            LOG.error("Fail to init index file {}.", this.path, t);
        }
    }

    /**
     * The offset entry of Index
     */
    public static class IndexEntry {
        private int offset;
        private int position;

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
     * The binary search algorithm is used to find the lower bound for the given target.
     */
    public int lowerBoundBinarySearch(final int begin, final int end, final int target) {
        int lo = begin;
        int hi = end;
        while (lo < hi) {
            final int mid = (lo + hi + 1) / 2;
            final IndexEntry entry = parseEntry(this.buffer, mid);
            if (target < entry.getOffset()) {
                hi = mid - 1;
            } else if (target >= entry.getOffset()) {
                lo = mid;
            }
        }
        return lo;
    }

    /**
     * Parse an entry from the index file
     */
    public abstract IndexEntry parseEntry(final ByteBuffer buffer, final int n);

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     */
    public int roundDownToExactMultiple(final int number, final int factor) {
        return factor * (number / factor);
    }

    /**
     * Flush data to disk
     */
    public void flush() {
        this.writeLock.lock();
        try {
            MmapUtil.fsync(this.buffer, this.path);
        } finally {
            this.writeLock.unlock();
        }
    }

}
