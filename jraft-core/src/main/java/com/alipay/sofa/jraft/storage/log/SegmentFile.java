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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage.WriteContext;
import com.alipay.sofa.jraft.storage.log.SegmentFile.SegmentFileOptions;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BufferUtils;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Utils;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

/**
 * A fixed size file. The content format is:
 * <pre>
 *   magic bytes       first log index    reserved
 *   [0x20 0x20]      [... 8 bytes...]   [8 bytes]
 *
 *   [record, record, ...]
 * <pre>
 *
 * Every record format is:
 * <pre>
 *   Magic bytes     data length   data
 *   [0x57, 0x8A]    [4 bytes]     [bytes]
 *</pre>
 *
 * @author boyan(boyan@antfin.com)
 * @since 1.2.6
 */
public class SegmentFile implements Lifecycle<SegmentFileOptions> {

    private static final int  FSYNC_COST_MS_THRESHOLD = 1000;
    private static final int  ONE_MINUTE              = 60 * 1000;
    public static final int   HEADER_SIZE             = 18;
    private static final long BLANK_LOG_INDEX         = -99;

    /**
     * Segment file header.
     * @author boyan(boyan@antfin.com)
     *
     */
    public static class SegmentHeader {

        private static final long RESERVED_FLAG = 0L;
        // The file first log index(inclusive)
        volatile long             firstLogIndex = BLANK_LOG_INDEX;
        @SuppressWarnings("unused")
        long                      reserved;
        private static final byte MAGIC         = 0x20;

        public SegmentHeader() {
            super();
        }

        ByteBuffer encode() {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
            buffer.put(MAGIC);
            buffer.put(MAGIC);
            buffer.putLong(this.firstLogIndex);
            buffer.putLong(RESERVED_FLAG);
            BufferUtils.flip(buffer);
            return buffer;
        }

        boolean decode(final ByteBuffer buffer) {
            if (buffer == null || buffer.remaining() < HEADER_SIZE) {
                LOG.error("Fail to decode segment header, invalid buffer length: {}",
                    buffer == null ? 0 : buffer.remaining());
                return false;
            }
            if (buffer.get() != MAGIC) {
                LOG.error("Fail to decode segment header, invalid magic.");
                return false;
            }
            if (buffer.get() != MAGIC) {
                LOG.error("Fail to decode segment header, invalid magic.");
                return false;
            }
            this.firstLogIndex = buffer.getLong();
            return true;
        }
    }

    /**
     * Segment file options.
     *
     * @author boyan(boyan@antfin.com)
     */
    public static class SegmentFileOptions {
        // Whether to recover
        final boolean recover;
        // Recover start position
        final int     pos;
        // True when is the last file.
        final boolean isLastFile;
        // True when is a new created file.
        final boolean isNewFile;
        final boolean sync;

        private SegmentFileOptions(final boolean recover, final boolean isLastFile, final boolean isNewFile,
                                   final boolean sync, final int pos) {
            super();
            this.isNewFile = isNewFile;
            this.isLastFile = isLastFile;
            this.recover = recover;
            this.sync = sync;
            this.pos = pos;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            boolean recover    = false;
            int     pos        = 0;
            boolean isLastFile = false;
            boolean isNewFile  = false;
            boolean sync       = true;

            public Builder setRecover(final boolean recover) {
                this.recover = recover;
                return this;
            }

            public Builder setPos(final int pos) {
                this.pos = pos;
                return this;
            }

            public Builder setLastFile(final boolean isLastFile) {
                this.isLastFile = isLastFile;
                return this;
            }

            public Builder setNewFile(final boolean isNewFile) {
                this.isNewFile = isNewFile;
                return this;
            }

            public Builder setSync(final boolean sync) {
                this.sync = sync;
                return this;
            }

            public SegmentFileOptions build() {
                return new SegmentFileOptions(this.recover, this.isLastFile, this.isNewFile, this.sync, this.pos);
            }
        }

    }

    private static final int         BLANK_HOLE_SIZE         = 64;

    private static final Logger      LOG                     = LoggerFactory.getLogger(SegmentFile.class);

    // 4 Bytes for written data length
    private static final int         RECORD_DATA_LENGTH_SIZE = 4;

    /**
     * Magic bytes for data buffer.
     */
    public static final byte[]       RECORD_MAGIC_BYTES      = new byte[] { (byte) 0x57, (byte) 0x8A };

    public static final int          RECORD_MAGIC_BYTES_SIZE = RECORD_MAGIC_BYTES.length;

    private final SegmentHeader      header;

    // The file last log index(inclusive)
    private volatile long            lastLogIndex            = Long.MAX_VALUE;
    // File size
    private int                      size;
    // File path
    private final String             path;
    // mmap byte buffer.
    private MappedByteBuffer         buffer;
    // Wrote position.
    private volatile int             wrotePos;
    // Committed position
    private volatile int             committedPos;

    private final ReadWriteLock      readWriteLock           = new ReentrantReadWriteLock(false);

    private final Lock               writeLock               = this.readWriteLock.writeLock();
    private final Lock               readLock                = this.readWriteLock.readLock();
    private final ThreadPoolExecutor writeExecutor;
    private volatile boolean         swappedOut;
    private volatile boolean         readOnly;
    private long                     swappedOutTimestamp     = -1L;
    private final String             filename;

    public SegmentFile(final int size, final String path, final ThreadPoolExecutor writeExecutor) {
        super();
        this.header = new SegmentHeader();
        this.size = size;
        this.writeExecutor = writeExecutor;
        this.path = path;
        this.filename = FilenameUtils.getName(this.path);
        this.swappedOut = this.readOnly = false;
    }

    void setReadOnly(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    void setFirstLogIndex(final long index) {
        this.header.firstLogIndex = index;
    }

    long getLastLogIndex() {
        return this.lastLogIndex;
    }

    @OnlyForTest
    public int getWrotePos() {
        return this.wrotePos;
    }

    int getCommittedPos() {
        return this.committedPos;
    }

    String getFilename() {
        return this.filename;
    }

    long getFirstLogIndex() {
        return this.header.firstLogIndex;
    }

    public boolean isSwappedOut() {
        return this.swappedOut;
    }

    int getSize() {
        return this.size;
    }

    /**
     * return true when this segment file is blank that we don't write any data into it.
     * @return
     */
    boolean isBlank() {
        return this.header.firstLogIndex == BLANK_LOG_INDEX;
    }

    boolean isHeaderCorrupted() {
        return this.header == null;
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

    private void swapIn() {
        if (this.swappedOut) {
            this.writeLock.lock();
            try {
                if (!this.swappedOut) {
                    return;
                }
                long startMs = Utils.monotonicMs();
                mmapFile(false);
                this.swappedOut = false;
                LOG.info("Swapped in segment file {} cost {} ms.", this.path, Utils.monotonicMs() - startMs);
            } finally {
                this.writeLock.unlock();
            }
        }
    }

    // Cached method for sun.nio.ch.DirectBuffer#address
    private static MethodHandle ADDRESS_METHOD = null;

    static {
        try {
            Class<?> clazz = Class.forName("sun.nio.ch.DirectBuffer");
            if (clazz != null) {
                Method method = clazz.getMethod("address");
                if (method != null) {
                    ADDRESS_METHOD = MethodHandles.lookup().unreflect(method);
                }
            }
        } catch (Throwable t) {
            // NOPMD
        }
    }

    private Pointer getPointer() {
        if (ADDRESS_METHOD != null) {
            try {
                final long address = (long) ADDRESS_METHOD.invoke(this.buffer);
                Pointer pointer = new Pointer(address);
                return pointer;
            } catch (Throwable t) {
                // NOPMD
            }
        }
        return null;
    }

    public void hintLoad() {
        Pointer pointer = getPointer();

        if (pointer != null) {
            long beginTime = Utils.monotonicMs();
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.size), LibC.MADV_WILLNEED);
            LOG.info("madvise(MADV_WILLNEED) {} {} {} ret = {} time consuming = {}", pointer, this.path, this.size,
                ret, Utils.monotonicMs() - beginTime);
        }
    }

    public void hintUnload() {
        Pointer pointer = getPointer();

        if (pointer != null) {
            long beginTime = Utils.monotonicMs();
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.size), LibC.MADV_DONTNEED);
            LOG.info("madvise(MADV_DONTNEED) {} {} {} ret = {} time consuming = {}", pointer, this.path, this.size,
                ret, Utils.monotonicMs() - beginTime);
        }
    }

    public void swapOut() {
        if (!this.swappedOut) {
            this.writeLock.lock();
            try {
                if (this.swappedOut) {
                    return;
                }
                if (!this.readOnly) {
                    LOG.warn("The segment file {} is not readonly, can't be swapped out.", this.path);
                    return;
                }
                final long now = Utils.monotonicMs();
                if (this.swappedOutTimestamp > 0 && now - this.swappedOutTimestamp < ONE_MINUTE) {
                    return;
                }
                this.swappedOut = true;
                Utils.unmap(this.buffer);
                this.buffer = null;
                this.swappedOutTimestamp = now;
                LOG.info("Swapped out segment file {} cost {} ms.", this.path, Utils.monotonicMs() - now);
            } finally {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * Truncate data from wrotePos(inclusive) to the file end and set lastLogIndex=logIndex.
     * @param wrotePos the wrote position(inclusive)
     * @param logIndex the log index
     * @param sync whether to call fsync
     */
    public void truncateSuffix(final int wrotePos, final long logIndex, final boolean sync) {
        this.writeLock.lock();
        try {
            if (wrotePos >= this.wrotePos) {
                return;
            }
            swapInIfNeed();
            final int oldPos = this.wrotePos;
            clear(wrotePos, sync);
            this.wrotePos = wrotePos;
            this.lastLogIndex = logIndex;
            BufferUtils.position(this.buffer, wrotePos);
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
            return logIndex >= this.header.firstLogIndex && logIndex <= this.lastLogIndex;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Clear data in [startPos, startPos+64).
     *
     * @param startPos the start position(inclusive)
     */
    public void clear(final int startPos, final boolean sync) {
        this.writeLock.lock();
        try {
            if (startPos < 0 || startPos > this.size) {
                return;
            }
            final int endPos = Math.min(this.size, startPos + BLANK_HOLE_SIZE);
            for (int i = startPos; i < endPos; i++) {
                this.buffer.put(i, (byte) 0);
            }
            if (sync) {
                fsync(this.buffer);
            }
            LOG.info("Segment file {} cleared data in [{}, {}).", this.path, startPos, endPos);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean init(final SegmentFileOptions opts) {
        if (opts.isNewFile) {
            return loadNewFile(opts);
        } else {
            return loadExistsFile(opts);
        }

    }

    private boolean loadNewFile(final SegmentFileOptions opts) {
        assert (opts.pos == 0);
        assert (!opts.recover);

        final File file = new File(this.path);

        if (file.exists()) {
            LOG.error("File {} already exists.", this.path);
            return false;
        }
        long startMs = Utils.monotonicMs();
        this.writeLock.lock();
        try (FileChannel fc = openFileChannel(true)) {
            this.buffer = fc.map(MapMode.READ_WRITE, 0, this.size);
            // Warmup mmap file
            BufferUtils.position(this.buffer, 0);
            BufferUtils.limit(this.buffer, this.size);
            saveHeader(true);

            this.committedPos = this.wrotePos = HEADER_SIZE;
            BufferUtils.position(this.buffer, this.wrotePos);

            assert (this.wrotePos == this.buffer.position());

            LOG.info("Created a new segment file {} cost {} ms, wrotePosition={}, bufferPosition={}, mappedSize={}.",
                this.path, Utils.monotonicMs() - startMs, this.wrotePos, this.buffer.position(), this.size);
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to init segment file {}.", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    private boolean loadExistsFile(final SegmentFileOptions opts) {
        this.writeLock.lock();
        try {
            if (!mmapFile(false)) {
                return false;
            }
            if (!tryRecoverExistsFile(opts)) {
                return false;
            }
            this.readOnly = !opts.isLastFile;
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    private boolean tryRecoverExistsFile(final SegmentFileOptions opts) {
        try {
            if (isBlank()) {
                // A blank segment, we don't need to recover.
                assert (!opts.recover);
                this.committedPos = this.wrotePos = HEADER_SIZE;
                BufferUtils.position(this.buffer, this.wrotePos);
                LOG.info("Segment file {} is blank, truncate it from {}.", this.path, HEADER_SIZE);
                clear(this.wrotePos, opts.sync);
            } else {
                if (opts.recover) {
                    if (!recover(opts)) {
                        return false;
                    }
                } else {
                    this.wrotePos = opts.pos;
                    BufferUtils.position(this.buffer, this.wrotePos);
                }
                assert (this.wrotePos == this.buffer.position());
                this.committedPos = this.wrotePos;
            }
            LOG.info("Loaded segment file {}, wrotePosition={}, bufferPosition={}, mappedSize={}.", this.path,
                this.wrotePos, this.buffer.position(), this.size);
        } catch (final Exception e) {
            LOG.error("Fail to load segment file {}.", this.path, e);
            return false;
        }
        return true;
    }

    boolean mmapFile(final boolean create) {
        if (this.buffer != null) {
            return true;
        }
        final File file = new File(this.path);

        if (file.exists()) {
            this.size = (int) file.length();
        } else {
            if (!create) {
                LOG.error("File {} is not exists.", this.path);
                return false;
            }
        }
        try (FileChannel fc = openFileChannel(create)) {
            this.buffer = fc.map(MapMode.READ_WRITE, 0, this.size);
            this.buffer.limit(this.size);
            if (!loadHeader()) {
                LOG.error("Fail to load segment header from file {}.", this.path);
                return false;
            }
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to mmap segment file {}.", this.path, e);
            return false;
        }
    }

    private FileChannel openFileChannel(final boolean create) throws IOException {
        if (create) {
            return FileChannel.open(Paths.get(this.path), StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        } else {
            return FileChannel.open(Paths.get(this.path), StandardOpenOption.READ, StandardOpenOption.WRITE);
        }
    }

    boolean loadHeader() {
        int oldPos = this.buffer.position();
        try {
            BufferUtils.position(this.buffer, 0);
            return this.header.decode(this.buffer.asReadOnlyBuffer());
        } finally {
            BufferUtils.position(this.buffer, oldPos);
        }
    }

    void saveHeader(final boolean sync) {
        int oldPos = this.buffer.position();
        try {
            BufferUtils.position(this.buffer, 0);
            final ByteBuffer headerBuf = this.header.encode();
            assert (headerBuf.remaining() == HEADER_SIZE);
            this.buffer.put(headerBuf);
            if (sync) {
                fsync(this.buffer);
            }
        } finally {
            BufferUtils.position(this.buffer, oldPos);
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private boolean recover(final SegmentFileOptions opts) throws IOException {
        LOG.info("Start to recover segment file {} from position {}.", this.path, opts.pos);
        this.wrotePos = opts.pos;
        if (this.wrotePos < HEADER_SIZE) {
            this.wrotePos = HEADER_SIZE;
        }
        BufferUtils.position(this.buffer, this.wrotePos);
        final long start = Utils.monotonicMs();
        while (this.wrotePos < this.size) {
            if (this.buffer.remaining() < RECORD_MAGIC_BYTES_SIZE) {
                LOG.error("Fail to recover segment file {}, missing magic bytes.", this.path);
                return false;
            }
            final byte[] magicBytes = new byte[RECORD_MAGIC_BYTES_SIZE];
            this.buffer.get(magicBytes);

            if (!Arrays.equals(RECORD_MAGIC_BYTES, magicBytes)) {

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
                    truncateFile(opts.sync);
                } else {
                    // Reach blank hole, rewind position.
                    BufferUtils.position(this.buffer, this.buffer.position() - RECORD_MAGIC_BYTES_SIZE);
                }
                // Reach end or dirty magic bytes, we should break out.
                break;
            }

            if (this.buffer.remaining() < RECORD_DATA_LENGTH_SIZE) {
                if (opts.isLastFile) {
                    LOG.error("Corrupted data length in segment file {} at pos={}, will truncate it.", this.path,
                        this.buffer.position());
                    truncateFile(opts.sync);
                    break;
                } else {
                    LOG.error(
                        "Fail to recover segment file {}, invalid data length remaining: {}, expected {} at pos={}.",
                        this.path, this.buffer.remaining(), RECORD_DATA_LENGTH_SIZE, this.wrotePos);
                    return false;
                }
            }

            final int dataLen = this.buffer.getInt();
            if (this.buffer.remaining() < dataLen) {
                if (opts.isLastFile) {
                    LOG.error(
                        "Corrupted data in segment file {} at pos={},  expectDataLength={}, but remaining is {}, will truncate it.",
                        this.path, this.buffer.position(), dataLen, this.buffer.remaining());
                    truncateFile(opts.sync);
                    break;
                } else {
                    LOG.error(
                        "Fail to recover segment file {}, invalid data: expected {} bytes in buf but actual {} at pos={}.",
                        this.path, dataLen, this.buffer.remaining(), this.wrotePos);
                    return false;
                }

            }
            // Skip data
            this.buffer.position(this.buffer.position() + dataLen);
            this.wrotePos += RECORD_MAGIC_BYTES_SIZE + RECORD_DATA_LENGTH_SIZE + dataLen;
        }
        LOG.info("Recover segment file {} cost {} millis.", this.path, Utils.monotonicMs() - start);
        return true;
    }

    private void truncateFile(final boolean sync) throws IOException {
        // Truncate dirty data.
        clear(this.wrotePos, sync);
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
        return RECORD_MAGIC_BYTES_SIZE + RECORD_DATA_LENGTH_SIZE + data.length;
    }

    /**
     * Write the data and return it's wrote position.
     *
     * @param logIndex the log index
     * @param data     data to write
     * @return the wrote position
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public int write(final long logIndex, final byte[] data, final WriteContext ctx) {
        int pos = -1;
        MappedByteBuffer buf = null;
        this.writeLock.lock();
        try {
            assert (this.wrotePos == this.buffer.position());
            buf = this.buffer;
            pos = this.wrotePos;
            this.wrotePos += RECORD_MAGIC_BYTES_SIZE + RECORD_DATA_LENGTH_SIZE + data.length;
            this.buffer.position(this.wrotePos);
            // Update log index.
            if (isBlank() || pos == HEADER_SIZE) {
                this.header.firstLogIndex = logIndex;
                // we don't need to call fsync header here, the new header will be flushed with this wrote.
                saveHeader(false);
            }
            this.lastLogIndex = logIndex;
            return pos;
        } finally {
            this.writeLock.unlock();
            final int wroteIndex = pos;
            final MappedByteBuffer buffer = buf;
            this.writeExecutor.execute(() -> {
                try {
                    put(buffer, wroteIndex, RECORD_MAGIC_BYTES);
                    putInt(buffer, wroteIndex + RECORD_MAGIC_BYTES_SIZE, data.length);
                    put(buffer, wroteIndex + RECORD_MAGIC_BYTES_SIZE + RECORD_DATA_LENGTH_SIZE, data);
                } catch (final Exception e) {
                    ctx.setError(e);
                } finally {
                    ctx.finishJob();
                }
            });
        }
    }

    private static void putInt(final MappedByteBuffer buffer, final int index, final int n) {
        byte[] bs = new byte[RECORD_DATA_LENGTH_SIZE];
        Bits.putInt(bs, 0, n);
        for (int i = 0; i < bs.length; i++) {
            buffer.put(index + i, bs[i]);
        }
    }

    private static void put(final MappedByteBuffer buffer, final int index, final byte[] data) {
        for (int i = 0; i < data.length; i++) {
            buffer.put(index + i, data[i]);
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
        assert (pos >= HEADER_SIZE);
        swapInIfNeed();
        this.readLock.lock();
        try {
            if (logIndex < this.header.firstLogIndex || logIndex > this.lastLogIndex) {
                LOG.warn(
                    "Try to read data from segment file {} out of range, logIndex={}, readPos={}, firstLogIndex={}, lastLogIndex={}.",
                    this.path, logIndex, pos, this.header.firstLogIndex, this.lastLogIndex);
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
            if (readBuffer.remaining() < RECORD_MAGIC_BYTES_SIZE) {
                throw new IOException("Missing magic buffer.");
            }
            readBuffer.position(pos + RECORD_MAGIC_BYTES_SIZE);
            final int dataLen = readBuffer.getInt();
            //TODO(boyan) reuse data array?
            final byte[] data = new byte[dataLen];
            readBuffer.get(data);
            return data;
        } finally {
            this.readLock.unlock();
        }
    }

    private void swapInIfNeed() {
        if (this.swappedOut) {
            swapIn();
        }
    }

    /**
     * Forces any changes made to this segment file's content to be written to the
     * storage device containing the mapped file.
     */
    public void sync(final boolean sync) throws IOException {
        MappedByteBuffer buf = null;
        this.writeLock.lock();
        try {
            if (this.committedPos >= this.wrotePos) {
                return;
            }
            this.committedPos = this.wrotePos;
            buf = this.buffer;
            LOG.debug("Commit segment file {} at pos {}.", this.path, this.committedPos);
        } finally {
            this.writeLock.unlock();
        }
        if (sync) {
            fsync(buf);
        }
    }

    private void fsync(final MappedByteBuffer buffer) {
        if (buffer != null) {
            long startMs = Utils.monotonicMs();
            buffer.force();
            final long cost = Utils.monotonicMs() - startMs;
            if (cost >= FSYNC_COST_MS_THRESHOLD) {
                LOG.warn("Call fsync on file {}  cost {} ms.", this.path, cost);
            }
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

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            if (this.buffer == null) {
                return;
            }
            hintUnload();
            Utils.unmap(this.buffer);
            this.buffer = null;
            LOG.info("Unloaded segment file {}, current status: {}.", this.path, toString());
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public String toString() {
        return "SegmentFile [firstLogIndex=" + this.header.firstLogIndex + ", lastLogIndex=" + this.lastLogIndex
               + ", size=" + this.size + ", path=" + this.path + ", wrotePos=" + this.wrotePos + ", committedPos="
               + this.committedPos + "]";
    }
}
