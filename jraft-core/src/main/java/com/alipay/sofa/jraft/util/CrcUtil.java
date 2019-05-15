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
package com.alipay.sofa.jraft.util;

/**
 * CRC utilities to compute CRC64 checksum.
 *
 * @author boyan(boyan@antfin.com)
 */
public final class CrcUtil {

    private static final ThreadLocal<CRC64> CRC_64_THREAD_LOCAL = ThreadLocal.withInitial(CRC64::new);

    /**
     * Compute CRC64 checksum for byte[].
     *
     * @param array source array
     * @return checksum value
     */
    public static long crc64(final byte[] array) {
        if (array != null) {
            return crc64(array, 0, array.length);
        }

        return 0;
    }

    /**
     * Compute CRC64 checksum for byte[].
     *
     * @param array  source array
     * @param offset starting position in the source array
     * @param length the number of array elements to be computed
     * @return checksum value
     */
    public static long crc64(final byte[] array, final int offset, final int length) {
        final CRC64 crc32 = CRC_64_THREAD_LOCAL.get();
        crc32.update(array, offset, length);
        final long ret = crc32.getValue();
        crc32.reset();
        return ret;
    }

    private CrcUtil() {
    }
}