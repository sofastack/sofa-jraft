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

/**
 *
 * @author jiachun.fjc
 */
@SuppressWarnings("all")
public final class VarInts {

    public static byte[] writeVarInt32(int value) {
        int position = 0;
        int size = computeRawVarInt32Size(value);
        byte[] bytes = new byte[size];
        while (true) {
            if ((value & ~0x7F) == 0) {
                bytes[position] = (byte) value;
                return bytes;
            } else {
                bytes[position++] = (byte) ((value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    public static int readVarInt32(byte[] bytes) {
        int position = 0;
        byte tmp = bytes[position++];
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = bytes[position++]) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = bytes[position++]) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = bytes[position++]) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = bytes[position++]) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (bytes[position++] >= 0) {
                                return result;
                            }
                        }
                        throw new RuntimeException("encountered a malformed varInt");
                    }
                }
            }
        }
        return result;
    }

    public static byte[] writeVarInt64(long value) {
        int position = 0;
        int size = computeRawVarInt64Size(value);
        byte[] bytes = new byte[size];
        while (true) {
            if ((value & ~0x7FL) == 0) {
                bytes[position] = (byte) value;
                return bytes;
            } else {
                bytes[position++] = (byte) (((int) value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    public static long readVarInt64(byte[] bytes) {
        int shift = 0;
        long result = 0;
        int position = 0;
        while (shift < 64) {
            final byte b = bytes[position++];
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new RuntimeException("encountered a malformed varInt");
    }

    /**
     * Compute the number of bytes that would be needed to encode a varInt.
     * {@code value} is treated as unsigned, so it won't be sign-extended
     * if negative.
     */
    public static int computeRawVarInt32Size(final int value) {
        if ((value & (~0 << 7)) == 0) {
            return 1;
        }
        if ((value & (~0 << 14)) == 0) {
            return 2;
        }
        if ((value & (~0 << 21)) == 0) {
            return 3;
        }
        if ((value & (~0 << 28)) == 0) {
            return 4;
        }
        return 5;
    }

    /**
     * Compute the number of bytes that would be needed to encode a varInt.
     */
    public static int computeRawVarInt64Size(long value) {
        // Handle two popular special cases up front ...
        if ((value & (~0L << 7)) == 0L) {
            return 1;
        }
        if (value < 0L) {
            return 10;
        }
        // ... leaving us with 8 remaining, which we can divide and conquer
        int n = 2;
        if ((value & (~0L << 35)) != 0L) {
            n += 4;
            value >>>= 28;
        }
        if ((value & (~0L << 21)) != 0L) {
            n += 2;
            value >>>= 14;
        }
        if ((value & (~0L << 14)) != 0L) {
            n += 1;
        }
        return n;
    }

    private VarInts() {
    }
}
