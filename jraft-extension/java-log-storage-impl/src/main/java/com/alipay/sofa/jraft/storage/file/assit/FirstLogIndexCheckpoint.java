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

import com.alipay.sofa.jraft.util.Bits;

/**
 * This checkpoint is used for save firstLogIndex
 * @author hzh (642256541@qq.com)
 */
public class FirstLogIndexCheckpoint extends Checkpoint {

    // LogStorage first log index
    public long firstLogIndex = -1;

    public FirstLogIndexCheckpoint(final String path) {
        super(path);
    }

    /**
     * firstLogIndex (8 bytes)
     */
    public byte[] encode() {
        byte[] bs = new byte[8];
        Bits.putLong(bs, 0, this.firstLogIndex);
        return bs;
    }

    public boolean decode(final byte[] bs) {
        if (bs.length < 8) {
            return false;
        }
        this.firstLogIndex = Bits.getLong(bs, 0);
        return this.firstLogIndex >= 0;
    }

    public void setFirstLogIndex(final long firstLogIndex) {
        this.firstLogIndex = firstLogIndex;
    }

    public void reset() {
        this.firstLogIndex = -1;
    }

    public boolean isInit() {
        return this.firstLogIndex >= 0;
    }

    @Override
    public String toString() {
        return "FirstLogIndexCheckpoint{" + "firstLogIndex=" + firstLogIndex + '}';
    }
}