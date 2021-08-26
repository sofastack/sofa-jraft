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
package com.alipay.sofa.jraft.rhea.fsm.pipe;

import java.util.BitSet;
import java.util.List;

/**
 * Specific bloomFilter, hash function size = 1, like bitmap
 * @author hzh (642256541@qq.com)
 */
public class BloomFilter {
    private final BitSet bitset;
    private final int    bitSetSize;

    public BloomFilter(final int bitSetSize) {
        this.bitSetSize = bitSetSize;
        this.bitset = new BitSet(bitSetSize);
    }

    public void add(final byte[] bytes) {
        final int hash = hash(bytes);
        bitset.set(Math.abs(hash % bitSetSize), true);
    }

    public void addAll(final List<byte[]> bytesList) {
        for (final byte[] bytes : bytesList) {
            if (bytes != null) {
                this.add(bytes);
            }
        }
    }

    public boolean contains(final byte[] bytes) {
        final int hash = hash(bytes);
        return this.bitset.get(Math.abs(hash % bitSetSize));
    }

    /**
     * Although this hash function is very simple, but it is useful
     * Compared with other hashes, this hash function has the lowest probability of conflict
     */
    public int hash(final byte[] data) {
        int result = 0;
        for (final byte ch : data) {
            result = result * 131 + ch;
        }
        return result;
    }

    /**
     * Returns true if the specified {@code BitSet} has any bits set to
     * {@code true} that are also set to {@code true} in this {@code BitSet}.
     */
    public boolean intersects(final BloomFilter filter) {
        return filter.getBitset().intersects(this.bitset);
    }

    public void clear() {
        this.bitset.clear();
    }

    public BitSet getBitset() {
        return bitset;
    }

}
