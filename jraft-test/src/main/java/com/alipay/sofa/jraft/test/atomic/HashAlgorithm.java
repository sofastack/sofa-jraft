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
package com.alipay.sofa.jraft.test.atomic;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;

/**
 * Hash algorithm
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2017-Nov-22 5:00:08 PM
 */
public enum HashAlgorithm {
    /**
     * CRC32_HASH as used by the perl API. This will be more consistent both
     * across multiple API users as well as java versions, but is mostly likely
     * significantly slower.
     */
    CRC32_HASH,
    /**
     * FNV hashes are designed to be fast while maintaining a low collision
     * rate. The FNV speed allows one to quickly hash lots of data while
     * maintaining a reasonable collision rate.
     * 
     * @see <a href="http://www.isthe.com/chongo/tech/comp/fnv/"></a>
     * @see <a href="http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash"></a>
     */
    FNV1_64_HASH,
    /** 
     * hash based on md5
     *  **/
    KETAMA_HASH;

    private static final long FNV_64_INIT  = 0xcbf29ce484222325L;
    private static final long FNV_64_PRIME = 0x100000001b3L;

    public long hash(final String k) {
        long rv = 0;
        switch (this) {
            case CRC32_HASH:
                // return (crc32(shift) >> 16) & 0x7fff;
                CRC32 crc32 = new CRC32();
                crc32.update(k.getBytes());
                rv = crc32.getValue() >> 16 & 0x7fff;
                break;
            case FNV1_64_HASH: {
                rv = FNV_64_INIT;
                int len = k.length();
                for (int i = 0; i < len; i++) {
                    rv *= FNV_64_PRIME;
                    rv ^= k.charAt(i);
                }
            }
                break;
            case KETAMA_HASH:
                byte[] bKey = computeMd5(k);
                rv = (long) (bKey[3] & 0xFF) << 24 | (long) (bKey[2] & 0xFF) << 16 | (long) (bKey[1] & 0xFF) << 8
                     | bKey[0] & 0xFF;
                break;

        }
        return rv;
    }

    private static ThreadLocal<MessageDigest> md5Local = new ThreadLocal<>();

    /**
     * Get the md5 of the given key.
     */
    public static byte[] computeMd5(String k) {
        MessageDigest md5 = md5Local.get();
        if (md5 == null) {
            try {
                md5 = MessageDigest.getInstance("MD5");
                md5Local.set(md5);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("MD5 not supported", e);
            }
        }
        md5.reset();
        md5.update(k.getBytes(StandardCharsets.UTF_8));
        return md5.digest();
    }
}
