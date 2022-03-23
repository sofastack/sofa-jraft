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
package com.alipay.sofa.jraft.storage.file.assit;

import com.alipay.sofa.jraft.util.AsciiStringUtil;
import com.alipay.sofa.jraft.util.Bits;

/**
 * This checkpoint is used for save flushPosition and last LogIndex
 * @author hzh (642256541@qq.com)
 */
public class FlushStatusCheckpoint extends Checkpoint {

    // Current file name
    public String fileName;
    // Current flush position.
    public long   flushPosition;
    // Last logIndex
    public long   lastLogIndex;

    public FlushStatusCheckpoint(final String path) {
        super(path);
    }

    /**
     * flushPosition (8 bytes) + lastLogIndex (8 bytes) + path(4 bytes len + string bytes)
     */
    public byte[] encode() {
        byte[] ps = AsciiStringUtil.unsafeEncode(this.fileName);
        byte[] bs = new byte[20 + ps.length];
        Bits.putLong(bs, 0, this.flushPosition);
        Bits.putLong(bs, 8, this.lastLogIndex);
        Bits.putInt(bs, 16, ps.length);
        System.arraycopy(ps, 0, bs, 20, ps.length);
        return bs;
    }

    public boolean decode(final byte[] bs) {
        if (bs.length < 20) {
            return false;
        }
        this.flushPosition = Bits.getLong(bs, 0);
        this.lastLogIndex = Bits.getLong(bs, 8);
        int len = Bits.getInt(bs, 16);
        this.fileName = AsciiStringUtil.unsafeDecode(bs, 20, len);
        return this.flushPosition >= 0 && this.lastLogIndex >= 0 && !this.fileName.isEmpty();
    }

    public void setFileName(final String fileName) {
        this.fileName = fileName;
    }

    public void setFlushPosition(final long flushPosition) {
        this.flushPosition = flushPosition;
    }

    public void setLastLogIndex(final long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    @Override
    public String toString() {
        return "FlushStatusCheckpoint{" + "segFilename='" + fileName + '\'' + ", flushPosition=" + flushPosition
               + ", lastLogIndex=" + lastLogIndex + '}';
    }
}
