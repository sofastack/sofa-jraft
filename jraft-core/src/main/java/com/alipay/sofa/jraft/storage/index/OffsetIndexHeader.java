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

import java.nio.ByteBuffer;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/6/29 22:07
 */
public class OffsetIndexHeader {

    private static final Logger LOG           = LoggerFactory.getLogger(OffsetIndexHeader.class);

    private static final int    HEADER_SIZE   = 10;

    private static final long   RESERVED_FLAG = 0L;

    @SuppressWarnings("unused")
    long                        reserved;

    private static final byte   MAGIC         = 0x20;

    public OffsetIndexHeader() {
        super();
    }

    ByteBuffer encode() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.put(MAGIC);
        buffer.put(MAGIC);
        buffer.putLong(RESERVED_FLAG);
        buffer.flip();
        return buffer;
    }

    boolean decode(final ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < HEADER_SIZE) {
            LOG.error("Fail to decode offsetIndex header, invalid buffer length: {}",
                buffer == null ? 0 : buffer.remaining());
            return false;
        }
        if (buffer.get() != MAGIC) {
            LOG.error("Fail to decode offsetIndex header, invalid magic.");
            return false;
        }
        if (buffer.get() != MAGIC) {
            LOG.error("Fail to decode offsetIndex header, invalid magic.");
            return false;
        }
        return true;
    }

    public static int getHeaderSize() {
        return HEADER_SIZE;
    }
}
