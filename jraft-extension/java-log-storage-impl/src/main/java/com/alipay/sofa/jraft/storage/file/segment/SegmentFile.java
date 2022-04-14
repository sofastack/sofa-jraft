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
package com.alipay.sofa.jraft.storage.file.segment;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.storage.file.AbstractFile;

/**
 *  * File header:
 *  * <pre>
 *  *   magic bytes       first log index   file from offset       reserved
 *  *   [0x20 0x20]      [... 8 bytes...]   [... 8 bytes...]   [... 8 bytes...]
 *  * <pre>
 *
 *  * Every record format is:
 *  * <pre>
 *   Magic bytes     data length   data
 *   [0x57, 0x8A]    [4 bytes]     [bytes]
 *  *</pre>
 *  *
 * @author hzh (642256541@qq.com)
 */
public class SegmentFile extends AbstractFile {

    private static final Logger LOG                     = LoggerFactory.getLogger(SegmentFile.class);

    /**
     * Magic bytes for data buffer.
     */
    public static final byte[]  RECORD_MAGIC_BYTES      = new byte[] { (byte) 0x57, (byte) 0x8A };

    public static final int     RECORD_MAGIC_BYTES_SIZE = RECORD_MAGIC_BYTES.length;

    // 4 Bytes for written data length
    private static final int    RECORD_DATA_LENGTH_SIZE = 4;

    public SegmentFile(final String filePath, final int fileSize) {
        super(filePath, fileSize, true);
    }

    /**
     *
     * Write the data and return it's wrote position.
     * @param logIndex the log index
     * @param data     data to write
     * @return the wrote position
     */
    public int appendData(final long logIndex, final byte[] data) {
        this.writeLock.lock();
        try {
            assert (logIndex > getLastLogIndex());
            final byte[] writeData = encodeData(data);
            return doAppend(logIndex, writeData);
        } finally {
            this.writeLock.unlock();
        }
    }

    private byte[] encodeData(final byte[] data) {
        ByteBuffer buffer = ByteBuffer.allocate(getWriteBytes(data));
        buffer.put(RECORD_MAGIC_BYTES);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();
        return buffer.array();
    }

    /**
     * Read data from the position.
     *
     * @param logIndex the log index
     * @param pos      the position to read
     * @return read data
     */
    public byte[] lookupData(final long logIndex, final int pos) {
        assert (pos >= this.header.getHeaderSize());
        mapInIfNecessary();
        this.readLock.lock();
        try {
            if (logIndex < this.header.getFirstLogIndex() || logIndex > this.getLastLogIndex()) {
                LOG.warn(
                    "Try to read data from segment file {} out of range, logIndex={}, readPos={}, firstLogIndex={}, lastLogIndex={}.",
                    getFilePath(), logIndex, pos, this.header.getFirstLogIndex(), getLastLogIndex());
                return null;
            }
            if (pos > getFlushedPosition()) {
                LOG.warn(
                    "Try to read data from segment file {} out of comitted position, logIndex={}, readPos={}, wrotePos={}, flushPos={}.",
                    getFilePath(), logIndex, pos, getWrotePosition(), getFlushedPosition());
                return null;
            }
            return lookupData(pos);
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Read data from the position
     * @param pos      the position to read
     * @return read data
     */
    public byte[] lookupData(final int pos) {
        assert (pos >= this.header.getHeaderSize());
        mapInIfNecessary();
        this.readLock.lock();
        try {
            final ByteBuffer readBuffer = sliceByteBuffer();
            readBuffer.position(pos);
            if (readBuffer.remaining() < RECORD_MAGIC_BYTES_SIZE) {
                return null;
            }
            final byte[] magic = new byte[RECORD_MAGIC_BYTES_SIZE];
            readBuffer.get(magic);
            if (!Arrays.equals(magic, RECORD_MAGIC_BYTES)) {
                return null;
            }
            final int dataLen = readBuffer.getInt();
            if (dataLen <= 0) {
                return null;
            }
            final byte[] data = new byte[dataLen];
            readBuffer.get(data);
            return data;
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
        // Check len
        if (buffer.remaining() < RECORD_DATA_LENGTH_SIZE) {
            return CheckDataResult.CHECK_FAIL;
        }
        final int dataLen = buffer.getInt();
        if (buffer.remaining() < dataLen) {
            return CheckDataResult.CHECK_FAIL;
        }
        final CheckDataResult result = CheckDataResult.CHECK_SUCCESS;
        result.setSize(RECORD_MAGIC_BYTES_SIZE + RECORD_DATA_LENGTH_SIZE + dataLen);
        return result;
    }

    @Override
    public void onRecoverDone(final int recoverPosition) {
        // Since the logs index in the segmentFile are discontinuous, we should set LastLogIndex by reading and deSerializing last entry log
        final ByteBuffer buffer = sliceByteBuffer();
        buffer.position(recoverPosition);
        final byte[] data = lookupData(recoverPosition);
        if (data != null) {
            final LogEntry lastEntry = LogEntryV2CodecFactory.getInstance().decoder().decode(data);
            if (lastEntry != null) {
                setLastLogIndex(lastEntry.getId().getIndex());
            }
        }
    }

    @Override
    public int truncate(final long logIndex, final int pos) {
        this.writeLock.lock();
        try {
            if (logIndex < this.header.getFirstLogIndex() || logIndex > this.header.getLastLogIndex()) {
                return 0;
            }
            if (pos < 0) {
                return getWrotePosition();
            }
            updateAllPosition(pos);
            clear(getWrotePosition());
            this.header.setLastLogIndex(logIndex - 1);
            return pos;
        } finally {
            this.writeLock.unlock();
        }
    }

    public static int getWriteBytes(final byte[] data) {
        return RECORD_MAGIC_BYTES_SIZE + RECORD_DATA_LENGTH_SIZE + data.length;
    }
}
