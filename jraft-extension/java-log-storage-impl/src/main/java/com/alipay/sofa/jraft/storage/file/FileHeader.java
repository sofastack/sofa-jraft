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
package com.alipay.sofa.jraft.storage.file;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File header:
 * <pre>
 *   magic bytes       first log index   file from offset       reserved
 *   [0x20 0x20]      [... 8 bytes...]   [... 8 bytes...]   [... 8 bytes...]
 * <pre>
 * @author hzh (642256541@qq.com)
 */
public class FileHeader {
    private static final Logger LOG                = LoggerFactory.getLogger(FileHeader.class);

    @SuppressWarnings("unused")
    private static final long   RESERVED_FLAG      = 0L;

    public static final int     HEADER_SIZE        = 26;

    public static final long    BLANK_OFFSET_INDEX = -99;

    private volatile long       FirstLogIndex      = BLANK_OFFSET_INDEX;

    private long                FileFromOffset     = -1;

    private volatile long       LastLogIndex       = BLANK_OFFSET_INDEX;

    private static final byte   MAGIC              = 0x20;

    public FileHeader() {
        super();
    }

    public ByteBuffer encode() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.put(MAGIC);
        buffer.put(MAGIC);
        buffer.putLong(this.FirstLogIndex);
        buffer.putLong(this.FileFromOffset);
        buffer.putLong(RESERVED_FLAG);
        buffer.flip();
        return buffer;
    }

    public boolean decode(final ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < HEADER_SIZE) {
            LOG.error("Fail to decode file header, invalid buffer length: {}", buffer == null ? 0 : buffer.remaining());
            return false;
        }
        if (buffer.get() != MAGIC) {
            return false;
        }
        if (buffer.get() != MAGIC) {
            return false;
        }
        this.FirstLogIndex = buffer.getLong();
        this.FileFromOffset = buffer.getLong();
        return true;
    }

    public long getFirstLogIndex() {
        return FirstLogIndex;
    }

    public void setFirstLogIndex(final long firstLogIndex) {
        this.FirstLogIndex = firstLogIndex;
    }

    public long getFileFromOffset() {
        return FileFromOffset;
    }

    public void setFileFromOffset(final long fileFromOffset) {
        this.FileFromOffset = fileFromOffset;
    }

    public long getLastLogIndex() {
        return LastLogIndex;
    }

    public void setLastLogIndex(final long lastLogIndex) {
        this.LastLogIndex = lastLogIndex;
    }

    public int getHeaderSize() {
        return HEADER_SIZE;
    }

    public boolean isBlank() {
        return this.FirstLogIndex == BLANK_OFFSET_INDEX;
    }
}
