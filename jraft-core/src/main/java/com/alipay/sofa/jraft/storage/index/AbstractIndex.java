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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author hzh
 */
public abstract class AbstractIndex {

    private Logger              LOG           = LoggerFactory.getLogger(AbstractIndex.class);

    // Length of one index
    private int                 entrySize     = 8;

    // File size
    private Long                size;

    // File path
    private final String        path;

    private final File          file;

    // mmap byte buffer.
    private MappedByteBuffer    buffer;

    private volatile int        entries;

    // Wrote position.
    private volatile int        wrotePos;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);
    private final Lock          writeLock     = this.readWriteLock.writeLock();
    private final Lock          readLock      = this.readWriteLock.readLock();

    public AbstractIndex(final String path) {
        this.path = path;
        this.file = new File(path);
        try {
            boolean isExist = file.createNewFile();
            try (RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
                this.size = raf.length();
                this.buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.size);
                if (!isExist) {
                    this.buffer.position(0);
                } else {
                    // If this file is existed , set position to last index entry
                    this.buffer.position(roundDownToExactMultiple(this.buffer.limit(), this.entrySize));
                }
            }
        } catch (final Throwable t) {
            LOG.error("Fail to init index file {}.", this.path, t);
        }
    }

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

    }

}
