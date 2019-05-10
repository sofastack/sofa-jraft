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
package com.alipay.sofa.jraft.storage.log;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.storage.log.SegmentFile.SegmentFileOptions;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Utils;

/**
 * A fixed size file.
 * @author boyan(boyan@antfin.com)
 * @since 1.2.6
 *
 */
public class SegmentFile implements Lifecycle<SegmentFileOptions> {

    private static final int DATA_LENGTH_SIZE = 4;

    /**
     * Segment file options.
     * @author boyan(boyan@antfin.com)
     *
     */
    public static class SegmentFileOptions {
        // Whether to recover
        public final boolean recover;
        // Recover start position
        public final int     pos;
        // True when is the last file.
        public final boolean isLastFile;

        public SegmentFileOptions(final boolean recover, final boolean isLastFile, final int pos) {
            super();
            this.isLastFile = isLastFile;
            this.recover = recover;
            this.pos = pos;
        }
    }

    public static final byte[]  MAGIC_BYTES      = new byte[] { 0x57, (byte) 0x8A };

    public static final int     MAGIC_BYTES_SIZE = MAGIC_BYTES.length;

    private static final Logger LOG              = LoggerFactory.getLogger(SegmentFile.class);

    // The file first log index in segments.
    private final long          firstLogIndex;
    private volatile long       lastLogIndex     = Long.MAX_VALUE;
    // File size
    private final int           size;
    // File path
    private final String        path;
    // mmap byte buffer.
    private MappedByteBuffer    buffer;
    // Wrote position.
    private volatile int        wrotePos;
    // Committed position
    private volatile int        committedPos;

    private final ReadWriteLock readWriteLock    = new ReentrantReadWriteLock(false);

    private final Lock          writeLock        = this.readWriteLock.writeLock();
    private final Lock          readLock         = this.readWriteLock.readLock();

    public SegmentFile(final long firstLogIndex, final int size, final String parentDir) {
        super();
        this.firstLogIndex = firstLogIndex;
        this.size = size;
        this.path = parentDir + File.separator + getSegmentFileName();
    }

    public String getSegmentFileName() {
        return String.format("%019d", this.firstLogIndex);
    }

    public long getLastLogIndex() {
        return this.lastLogIndex;
    }

    public void setLastLogIndex(final long lastLogIndex) {
        this.writeLock.lock();
        try {
            this.lastLogIndex = lastLogIndex;
        } finally {
            this.writeLock.unlock();
        }
    }

    public int getWrotePos() {
        return this.wrotePos;
    }

    public int getCommittedPos() {
        return this.committedPos;
    }

    public long getFirstLogIndex() {
        return this.firstLogIndex;
    }

    public int getSize() {
        return this.size;
    }

    public String getPath() {
        return this.path;
    }

    public void truncateSuffix(final int wrotePos, final long logIndex) throws IOException {
        this.writeLock.lock();
        try {
            int oldPos = this.wrotePos;
            this.wrotePos = wrotePos;
            this.lastLogIndex = logIndex;
            clearData(wrotePos, this.size);
            LOG.info(
                "Segment file {} truncate suffix from pos={}, then set lastLogIndex={}, oldWrotePos={}, newWrotePos={}",
                this.path, wrotePos, logIndex, oldPos, this.wrotePos);
        } finally {
            this.writeLock.unlock();
        }
    }

    public boolean contains(final long logIndex) {
        return logIndex >= this.firstLogIndex && logIndex <= this.lastLogIndex;
    }

    /**
     * Clear data in [startPos, endPos)
     * @param startPos
     * @param endPos
     * @throws IOException
     */
    public void clearData(final int startPos, int endPos) throws IOException {
        if (startPos < 0 || startPos > this.size) {
            return;
        }
        endPos = Math.min(this.size, endPos);
        for (int i = startPos; i < endPos; i++) {
            this.buffer.put(i, (byte) 0);
        }
        sync();
        LOG.info("Segment file {} cleared data in [{}, {}).", this.path, startPos, endPos);
    }

    @Override
    public boolean init(final SegmentFileOptions opts) {

        this.writeLock.lock();

        if (this.buffer != null) {
            this.writeLock.unlock();
            LOG.warn("Segment file {} already initialized.", this.path);
            return true;
        }

        try (FileChannel fc = FileChannel.open(Paths.get(this.path), StandardOpenOption.CREATE,
            StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            this.buffer = fc.map(MapMode.READ_WRITE, 0, this.size);
            this.buffer.limit(this.size);
            if (opts.recover) {
                if (!recover(opts)) {
                    return false;
                }
            } else {
                this.wrotePos = opts.pos;
                this.buffer.position(this.wrotePos);
            }
            assert (this.wrotePos == this.buffer.position());
            this.committedPos = this.wrotePos;
            LOG.info("Loaded segment file {}, wrotePosition={}.", this.path, this.wrotePos);
            return true;
        } catch (IOException e) {
            LOG.error("Fail to init segment file {}.", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }

    }

    private boolean recover(final SegmentFileOptions opts) throws IOException {
        this.wrotePos = opts.pos;
        this.buffer.position(this.wrotePos);
        long start = Utils.monotonicMs();
        while (this.wrotePos < this.size) {
            if (this.buffer.remaining() < MAGIC_BYTES_SIZE) {
                LOG.error("Fail to recover segment file {}, missing magic buffer.", this.path);
                return false;
            }
            byte[] magicBytes = new byte[MAGIC_BYTES_SIZE];
            this.buffer.get(magicBytes);

            if (!Arrays.equals(MAGIC_BYTES, magicBytes)) {

                boolean truncateDirty = false;

                for (byte b : magicBytes) {
                    if (b != 0) {
                        if (opts.isLastFile) {
                            // It's the last file
                            // Truncate the dirty data from wrotePos
                            truncateDirty = true;
                            break;
                        } else {
                            // It's not the last file, but has invalid magic bytes, the data is corrupted.
                            LOG.error("Fail to recover segment file {}, invalid magic bytes: {} at pos={}.", this.path,
                                BytesUtil.toHex(magicBytes), this.wrotePos);
                            return false;
                        }
                    }
                }

                if (truncateDirty) {
                    // Truncate dirty data.
                    clearData(this.wrotePos, this.size);
                    this.buffer.position(this.wrotePos);
                    LOG.info("Truncated segment file {} from pos={}.", this.path, this.wrotePos);
                } else {
                    // Reach blank hole, rewind position.
                    this.buffer.position(this.buffer.position() - MAGIC_BYTES_SIZE);
                }
                // Magic bytes mismatch ,break out.
                break;
            }

            if (this.buffer.remaining() < DATA_LENGTH_SIZE) {
                LOG.error("Fail to recover segment file {}, coruppted data length at pos={}.", this.path,
                    this.buffer.position());
                return false;
            }

            int dataLen = this.buffer.getInt();
            if (this.buffer.remaining() < dataLen) {
                LOG.error("Fail to recover segment file {}, coruppted data, expectDataLength={}, but is {}.",
                    this.path, dataLen, this.buffer.remaining());
                return false;
            }
            // Skip data
            this.buffer.position(this.buffer.position() + dataLen);
            this.wrotePos += MAGIC_BYTES_SIZE + DATA_LENGTH_SIZE + dataLen;
        }
        LOG.info("Recover segment file {} cost {} millis.", this.path, Utils.monotonicMs() - start);
        return true;
    }

    public boolean isFull(final long waitToWroteSize) {
        this.readLock.lock();
        try {
            return this.wrotePos + waitToWroteSize >= this.size;
        } finally {
            this.readLock.unlock();
        }
    }

    public static int writeSize(final byte[] data) {
        return MAGIC_BYTES_SIZE + DATA_LENGTH_SIZE + data.length;
    }

    /**
     * Write the data and return it's wrote position.
     * @param data
     * @return
     */
    public int write(final long logIndex, final byte[] data) {
        this.writeLock.lock();
        try {
            int pos = this.wrotePos;
            this.buffer.put(MAGIC_BYTES);
            this.buffer.putInt(data.length);
            this.buffer.put(data);
            this.wrotePos += MAGIC_BYTES_SIZE + DATA_LENGTH_SIZE + data.length;

            this.lastLogIndex = logIndex;
            return pos;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Read data from the position.
     * @param pos
     * @return
     */
    public byte[] read(final long logIndex, final int pos) throws IOException {
        this.readLock.lock();
        try {
            if (logIndex < this.firstLogIndex || logIndex > this.lastLogIndex) {
                return null;
            }
            if (pos >= this.committedPos) {
                return null;
            }
            ByteBuffer readBuffer = this.buffer.asReadOnlyBuffer();
            readBuffer.position(pos);
            if (readBuffer.remaining() < MAGIC_BYTES_SIZE) {
                throw new IOException("Missing magic buffer.");
            }
            readBuffer.position(pos + MAGIC_BYTES_SIZE);
            int dataLen = readBuffer.getInt();
            byte[] data = new byte[dataLen];
            readBuffer.get(data);
            return data;
        } finally {
            this.readLock.unlock();
        }
    }

    public void sync() throws IOException {
        this.writeLock.lock();
        try {
            if (this.committedPos == this.wrotePos) {
                return;
            }
            if (this.buffer != null) {
                this.buffer.force();
                this.committedPos = this.wrotePos;
                LOG.debug("Commit segment file {} to pos {}.", this.path, this.committedPos);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Destroy the file.
     */
    public void destroy() {
        this.writeLock.lock();
        try {
            shutdown();
            FileUtils.deleteQuietly(new File(this.path));
            LOG.info("Deleted segment file {}.", this.path);
        } finally {
            this.writeLock.unlock();
        }
    }

    // See https://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void closeDirectBuffer(MappedByteBuffer cb) {
        // JavaSpecVer: 1.6, 1.7, 1.8, 9, 10
        boolean isOldJDK = System.getProperty("java.specification.version", "99").startsWith("1.");
        try {
            if (isOldJDK) {
                Method cleaner = cb.getClass().getMethod("cleaner");
                cleaner.setAccessible(true);
                Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
                clean.setAccessible(true);
                clean.invoke(cleaner.invoke(cb));
            } else {
                Class unsafeClass;
                try {
                    unsafeClass = Class.forName("sun.misc.Unsafe");
                } catch (Exception ex) {
                    // jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
                    // but that method should be added if sun.misc.Unsafe is removed.
                    unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
                }
                Method clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
                clean.setAccessible(true);
                Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
                theUnsafeField.setAccessible(true);
                Object theUnsafe = theUnsafeField.get(null);
                clean.invoke(theUnsafe, cb);
            }
        } catch (Exception ex) {
        }
        cb = null;
    }

    @Override
    public String toString() {
        return "SegmentFile [firstLogIndex=" + this.firstLogIndex + ", lastLogIndex=" + this.lastLogIndex + ", size="
               + this.size + ", path=" + this.path + ", wrotePos=" + this.wrotePos + ", committedPos="
               + this.committedPos + "]";
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            if (this.buffer == null) {
                return;
            }
            closeDirectBuffer(this.buffer);
            LOG.info("Unloaded segment file {}, wrotePos={}.", this.path, this.wrotePos);
        } finally {
            this.writeLock.unlock();
        }
    }

}
