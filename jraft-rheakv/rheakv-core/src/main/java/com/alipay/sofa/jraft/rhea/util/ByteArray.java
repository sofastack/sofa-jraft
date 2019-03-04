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
package com.alipay.sofa.jraft.rhea.util;

import java.io.Serializable;
import java.util.Arrays;

import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Can use it as a map key,
 *
 * @author jiachun.fjc
 */
public final class ByteArray implements Comparable<ByteArray>, Serializable {

    private static final long serialVersionUID = 3030232535108421145L;

    private final byte[]      bytes;
    // Cache the hash code, default to 0
    private int               hashCode;

    public static ByteArray wrap(final byte[] bytes) {
        return new ByteArray(bytes);
    }

    ByteArray(byte[] bytes) {
        Requires.requireNonNull(bytes, "bytes");
        this.bytes = bytes;
        // Initialize hash code to 0
        this.hashCode = 0;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ByteArray that = (ByteArray) o;
        // We intentionally use the function to compute hashcode here
        return hashCode() == that.hashCode() && Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) { // Lazy initialize
            hashCode = Arrays.hashCode(bytes);
        }
        return hashCode;
    }

    @Override
    public int compareTo(ByteArray o) {
        return BytesUtil.compare(this.bytes, o.bytes);
    }
}
