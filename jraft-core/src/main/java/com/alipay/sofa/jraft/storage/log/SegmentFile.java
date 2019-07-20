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
 *
 * @author boyan(boyan@antfin.com)
 * @since 1.2.6
 */
public class SegmentFile implements Lifecycle<SegmentFileOptions> {

    private static final int    BLANK_HOLE_SIZE  = 64;

    private static final Logger LOG              = LoggerFactory.getLogger(SegmentFile.class);

    // 4 Bytes for written data length
    private static final int    DATA_LENGTH_SIZE = 4;

    /**
     * Segment file options.
     *
     * @author boyan(boyan@antfin.com)
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

        @Override
        public String toString() {
            return "SegmentFileOptions [recover=" + recover + ", pos=" + pos + ", isLastFile=" + isLastFile + "]";
        }
    }

    /**
     * Magic bytes for data buffer.
     */
    public static final byte[]  MAGIC_BYTES      = new byte[] { (byte) 0x57, (byte) 0x8A };

    public static final int     MAGIC_BYTES_SIZE = MAGIC_BYTES.length;

    // The file first log index(inclusive)
    private final long          firstLogIndex;
    // The file last log index(inclusive)
    private volatile long       lastLogIndex     = Long.MAX_VALUE;
    // File size
    private int                 size;
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
        this.path = parentDir + File.separator + getSegmentFileName(this.firstLogIndex);
    }

    static String getSegmentFileName(long logIndex) {
        return String.format("%019d", logIndex);
    }

    long getLastLogIndex() {
        return this.lastLogIndex;
    }

    int getWrotePos() {
        return this.wrotePos;
    }

    int getCommittedPos() {
        return this.committedPos;
    }

    long getFirstLogIndex() {
        return this.firstLogIndex;
    }

    int getSize() {
        return this.size;
    }

    String getPath() {
        return this.path;
    }

    public void setLastLogIndex(final long lastLogIndex) {
        this.writeLock.lock();
        try {
            this.lastLogIndex = lastLogIndex;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Truncate data from wrotePos(inclusive) to the file end and set lastLogIndex=logIndex.
     * @param wrotePos the wrote position(inclusive)
     * @param logIndex the log index
     */
    public void truncateSuffix(final int wrotePos, final long logIndex) {
        this.writeLock.lock();
        try {
            final int oldPos = this.wrotePos;
            clear(wrotePos);
            this.wrotePos = wrotePos;
            this.lastLogIndex = logIndex;
            this.buffer.position(wrotePos);
            LOG.info(
                "Segment file {} truncate suffix from pos={}, then set lastLogIndex={}, oldWrotePos={}, newWrotePos={}",
                this.path, wrotePos, logIndex, oldPos, this.wrotePos);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Returns true when the segment file contains the log index.
     *
     * @param logIndex the log index
     * @return true if the segment file contains the log index, otherwise return false
     */
    public boolean contains(final long logIndex) {
        this.readLock.lock();
        try {
            return logIndex >= this.firstLogIndex && logIndex <= this.lastLogIndex;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Clear data in [startPos, startPos+64).
     *
     * @param startPos the start position(inclusive)
     */
    public void clear(final int startPos) {
        this.writeLock.lock();
        try {
            if (startPos < 0 || startPos > this.size) {
                return;
            }
            final int endPos = Math.min(this.size, startPos + BLANK_HOLE_SIZE);
            for (int i = startPos; i < endPos; i++) {
                this.buffer.put(i, (byte) 0);
            }
            fsync();
            LOG.info("Segment file {} cleared data in [{}, {}).", this.path, startPos, endPos);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean init(final SegmentFileOptions opts) {
        this.writeLock.lock();
        if (this.buffer != null) {
            this.writeLock.unlock();
            LOG.warn("Segment file {} already initialized, the status: {}.", this.path, toString());
            return true;
        }

        final File file = new File(this.path);

        if (file.exists()) {
            this.size = (int) file.length();
        }

        try (FileChannel fc = openFileChannel(opts)) {
            if (opts.isLastFile) {
                this.buffer = fc.map(MapMode.READ_WRITE, 0, this.size);
            } else {
                this.buffer = fc.map(MapMode.READ_ONLY, 0, this.size);
            }

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
            LOG.info("Loaded segment file {}, wrotePosition={}, bufferPosition={}, mappedSize={}.", this.path,
                this.wrotePos, this.buffer.position(), this.size);
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to init segment file {}.", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }

    }

    private FileChannel openFileChannel(final SegmentFileOptions opts) throws IOException {
        if (opts.isLastFile) {
            return FileChannel.open(Paths.get(this.path), StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        } else {
            return FileChannel.open(Paths.get(this.path), StandardOpenOption.READ);
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private boolean recover(final SegmentFileOptions opts) throws IOException {
        LOG.info("Start to recover segment file {} from position {}.", this.path, opts.pos);
        this.wrotePos = opts.pos;
        this.buffer.position(this.wrotePos);
        final long start = Utils.monotonicMs();
        while (this.wrotePos < this.size) {
            if (this.buffer.remaining() < MAGIC_BYTES_SIZE) {
                LOG.error("Fail to recover segment file {}, missing magic bytes.", this.path);
                return false;
            }
            final byte[] magicBytes = new byte[MAGIC_BYTES_SIZE];
            this.buffer.get(magicBytes);

            if (!Arrays.equals(MAGIC_BYTES, magicBytes)) {

                boolean truncateDirty = false;

                int i = 0;
                for (final byte b : magicBytes) {
                    i++;
                    if (b != 0) {
                        if (opts.isLastFile) {
                            // It's the last file
                            // Truncate the dirty data from wrotePos
                            LOG.error("Corrupted magic bytes in segment file {} at pos={}, will truncate it.",
                                this.path, this.wrotePos + i);
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
                    truncateFile();
                } else {
                    // Reach blank hole, rewind position.
                    this.buffer.position(this.buffer.position() - MAGIC_BYTES_SIZE);
                }
                // Reach end or dirty magic bytes, we should break out.
                break;
            }

            if (this.buffer.remaining() < DATA_LENGTH_SIZE) {
                LOG.error("Corrupted data length in segment file {} at pos={}, will truncate it.", this.path,
                    this.buffer.position());
                truncateFile();
                break;
            }

            final int dataLen = this.buffer.getInt();
            if (this.buffer.remaining() < dataLen) {
                LOG.error(
                    "Corrupted data in segment file {} at pos={},  expectDataLength={}, but remaining is {}, will truncate it.",
                    this.path, this.buffer.position(), dataLen, this.buffer.remaining());
                truncateFile();
                break;
            }
            // Skip data
            this.buffer.position(this.buffer.position() + dataLen);
            this.wrotePos += MAGIC_BYTES_SIZE + DATA_LENGTH_SIZE + dataLen;
        }
        LOG.info("Recover segment file {} cost {} millis.", this.path, Utils.monotonicMs() - start);
        return true;
    }

    private void truncateFile() throws IOException {
        // Truncate dirty data.
        clear(this.wrotePos);
        this.buffer.position(this.wrotePos);
        LOG.warn("Truncated segment file {} from pos={}.", this.path, this.wrotePos);
    }

    public boolean reachesFileEndBy(final long waitToWroteBytes) {
        this.readLock.lock();
        try {
            return this.wrotePos + waitToWroteBytes > this.size;
        } finally {
            this.readLock.unlock();
        }
    }

    public boolean isFull() {
        return reachesFileEndBy(1);
    }

    static int getWriteBytes(final byte[] data) {
        return MAGIC_BYTES_SIZE + DATA_LENGTH_SIZE + data.length;
    }

    /**
     * Write the data and return it's wrote position.
     *
     * @param logIndex the log index
     * @param data     data to write
     * @return the wrote position
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public int write(final long logIndex, final byte[] data) {
        this.writeLock.lock();
        try {
            assert (this.wrotePos == this.buffer.position());
            final int pos = this.wrotePos;

            this.buffer.put(MAGIC_BYTES);
            this.buffer.putInt(data.length);
            this.buffer.put(data);
            this.wrotePos += MAGIC_BYTES_SIZE + DATA_LENGTH_SIZE + data.length;
            // Update last log index.
            this.lastLogIndex = logIndex;
            return pos;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Read data from the position.
     *
     * @param logIndex the log index
     * @param pos      the position to read
     * @return read data
     */
    public byte[] read(final long logIndex, final int pos) throws IOException {
        this.readLock.lock();
        try {
            if (logIndex < this.firstLogIndex || logIndex > this.lastLogIndex) {
                LOG.warn(
                    "Try to read data from segment file {} out of range, logIndex={}, readPos={}, firstLogIndex={}, lastLogIndex={}.",
                    this.path, logIndex, pos, this.firstLogIndex, this.lastLogIndex);
                return null;
            }
            if (pos >= this.committedPos) {
                LOG.warn(
                    "Try to read data from segment file {} out of comitted position, logIndex={}, readPos={}, wrotePos={}, this.committedPos={}.",
                    this.path, logIndex, pos, this.wrotePos, this.committedPos);
                return null;
            }
            final ByteBuffer readBuffer = this.buffer.asReadOnlyBuffer();
            readBuffer.position(pos);
            if (readBuffer.remaining() < MAGIC_BYTES_SIZE) {
                throw new IOException("Missing magic buffer.");
            }
            readBuffer.position(pos + MAGIC_BYTES_SIZE);
            final int dataLen = readBuffer.getInt();
            final byte[] data = new byte[dataLen];
            readBuffer.get(data);
            return data;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Forces any changes made to this segment file's content to be written to the
     * storage device containing the mapped file.
     */
    public void sync() throws IOException {
        if (this.committedPos >= this.wrotePos) {
            return;
        }
        this.writeLock.lock();
        try {
            // double check
            if (this.committedPos >= this.wrotePos) {
                return;
            }
            fsync();
            this.committedPos = this.wrotePos;
            LOG.debug("Commit segment file {} at pos {}.", this.path, this.committedPos);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void fsync() {
        if (this.buffer != null) {
            this.buffer.force();
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
    // TODO move into utils
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void closeDirectBuffer(final MappedByteBuffer cb) {
        // JavaSpecVer: 1.6, 1.7, 1.8, 9, 10
        final boolean isOldJDK = System.getProperty("java.specification.version", "99").startsWith("1.");
        try {
            if (isOldJDK) {
                final Method cleaner = cb.getClass().getMethod("cleaner");
                cleaner.setAccessible(true);
                final Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
                clean.setAccessible(true);
                clean.invoke(cleaner.invoke(cb));
            } else {
                Class unsafeClass;
                try {
                    unsafeClass = Class.forName("sun.misc.Unsafe");
                } catch (final Exception ex) {
                    // jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
                    // but that method should be added if sun.misc.Unsafe is removed.
                    unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
                }
                final Method clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
                clean.setAccessible(true);
                final Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
                theUnsafeField.setAccessible(true);
                final Object theUnsafe = theUnsafeField.get(null);
                clean.invoke(theUnsafe, cb);
            }
        } catch (final Exception ex) {
            LOG.error("Fail to un-mapped segment file.", ex);
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            if (this.buffer == null) {
                return;
            }
            closeDirectBuffer(this.buffer);
            this.buffer = null;
            LOG.info("Unloaded segment file {}, current status: {}.", this.path, toString());
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public String toString() {
        return "SegmentFile [firstLogIndex=" + this.firstLogIndex + ", lastLogIndex=" + this.lastLogIndex + ", size="
               + this.size + ", path=" + this.path + ", wrotePos=" + this.wrotePos + ", committedPos="
               + this.committedPos + "]";
    }
}
