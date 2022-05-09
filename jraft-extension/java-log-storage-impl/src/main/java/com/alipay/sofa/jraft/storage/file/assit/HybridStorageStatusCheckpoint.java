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
 * This checkpoint is used for saving whether the oldStorage in HybridStorage is shutdown
 *
 */
public class HybridStorageStatusCheckpoint extends Checkpoint {
    public boolean isOldStorageExist = true;

    public HybridStorageStatusCheckpoint(final String path) {
        super(path);
    }

    @Override
    public byte[] encode() {
        byte[] bs = new byte[2];
        short val = (short) (this.isOldStorageExist ? 1 : 0);
        Bits.putShort(bs, 0, val);
        return bs;
    }

    @Override
    public boolean decode(final byte[] bs) {
        if (bs.length < 1) {
            return false;
        }
        short val = Bits.getShort(bs, 0);
        this.isOldStorageExist = val == 1;
        return true;
    }
}
