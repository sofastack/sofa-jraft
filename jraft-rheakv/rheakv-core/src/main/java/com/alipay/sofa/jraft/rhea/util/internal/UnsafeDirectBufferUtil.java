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
package com.alipay.sofa.jraft.rhea.util.internal;

import java.lang.reflect.Method;
import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.internal.UnsafeUtil;

/**
 *
 * @author jiachun.fjc
 */
public final class UnsafeDirectBufferUtil {

    private static final Logger                    LOG                           = LoggerFactory
                                                                                     .getLogger(UnsafeDirectBufferUtil.class);

    private static final UnsafeUtil.UnsafeAccessor UNSAFE_ACCESSOR               = UnsafeUtil.getUnsafeAccessor();

    private static final long                      BYTE_ARRAY_BASE_OFFSET        = UnsafeUtil
                                                                                     .arrayBaseOffset(byte[].class);

    // Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to allow safepoint polling
    // during a large copy.
    private static final long                      UNSAFE_COPY_THRESHOLD         = 1024L * 1024L;

    // These numbers represent the point at which we have empirically
    // determined that the average cost of a JNI call exceeds the expense
    // of an element by element copy.  These numbers may change over time.
    private static final int                       JNI_COPY_TO_ARRAY_THRESHOLD   = 6;
    private static final int                       JNI_COPY_FROM_ARRAY_THRESHOLD = 6;

    private static final boolean                   BIG_ENDIAN_NATIVE_ORDER       = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
    // Unaligned-access capability
    private static final boolean                   UNALIGNED;

    static {
        boolean _unaligned;
        try {
            Class<?> bitsClass = Class.forName("java.nio.Bits", false, UnsafeUtil.getSystemClassLoader());
            Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
            unalignedMethod.setAccessible(true);
            _unaligned = (boolean) unalignedMethod.invoke(null);
        } catch (Throwable t) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("java.nio.Bits: unavailable, {}.", StackTraceUtil.stackTrace(t));
            }

            _unaligned = false;
        }
        UNALIGNED = _unaligned;
    }

    public static byte getByte(long address) {
        return UNSAFE_ACCESSOR.getByte(address);
    }

    public static short getShort(long address) {
        if (UNALIGNED) {
            short v = UNSAFE_ACCESSOR.getShort(address);
            return BIG_ENDIAN_NATIVE_ORDER ? v : Short.reverseBytes(v);
        }
        return (short) (UNSAFE_ACCESSOR.getByte(address) << 8 | UNSAFE_ACCESSOR.getByte(address + 1) & 0xff);
    }

    public static short getShortLE(long address) {
        if (UNALIGNED) {
            short v = UNSAFE_ACCESSOR.getShort(address);
            return BIG_ENDIAN_NATIVE_ORDER ? Short.reverseBytes(v) : v;
        }
        return (short) (UNSAFE_ACCESSOR.getByte(address) & 0xff | UNSAFE_ACCESSOR.getByte(address + 1) << 8);
    }

    public static int getInt(long address) {
        if (UNALIGNED) {
            int v = UNSAFE_ACCESSOR.getInt(address);
            return BIG_ENDIAN_NATIVE_ORDER ? v : Integer.reverseBytes(v);
        }
        return UNSAFE_ACCESSOR.getByte(address) << 24 //
               | (UNSAFE_ACCESSOR.getByte(address + 1) & 0xff) << 16 //
               | (UNSAFE_ACCESSOR.getByte(address + 2) & 0xff) << 8 //
               | UNSAFE_ACCESSOR.getByte(address + 3) & 0xff;
    }

    public static int getIntLE(long address) {
        if (UNALIGNED) {
            int v = UNSAFE_ACCESSOR.getInt(address);
            return BIG_ENDIAN_NATIVE_ORDER ? Integer.reverseBytes(v) : v;
        }
        return UNSAFE_ACCESSOR.getByte(address) & 0xff //
               | (UNSAFE_ACCESSOR.getByte(address + 1) & 0xff) << 8 //
               | (UNSAFE_ACCESSOR.getByte(address + 2) & 0xff) << 16 //
               | UNSAFE_ACCESSOR.getByte(address + 3) << 24;
    }

    public static long getLong(long address) {
        if (UNALIGNED) {
            long v = UNSAFE_ACCESSOR.getLong(address);
            return BIG_ENDIAN_NATIVE_ORDER ? v : Long.reverseBytes(v);
        }
        return ((long) UNSAFE_ACCESSOR.getByte(address)) << 56 //
               | (UNSAFE_ACCESSOR.getByte(address + 1) & 0xffL) << 48 //
               | (UNSAFE_ACCESSOR.getByte(address + 2) & 0xffL) << 40 //
               | (UNSAFE_ACCESSOR.getByte(address + 3) & 0xffL) << 32 //
               | (UNSAFE_ACCESSOR.getByte(address + 4) & 0xffL) << 24 //
               | (UNSAFE_ACCESSOR.getByte(address + 5) & 0xffL) << 16 //
               | (UNSAFE_ACCESSOR.getByte(address + 6) & 0xffL) << 8 //
               | (UNSAFE_ACCESSOR.getByte(address + 7)) & 0xffL;
    }

    public static long getLongLE(long address) {
        if (UNALIGNED) {
            long v = UNSAFE_ACCESSOR.getLong(address);
            return BIG_ENDIAN_NATIVE_ORDER ? Long.reverseBytes(v) : v;
        }
        return (UNSAFE_ACCESSOR.getByte(address)) & 0xffL //
               | (UNSAFE_ACCESSOR.getByte(address + 1) & 0xffL) << 8 //
               | (UNSAFE_ACCESSOR.getByte(address + 2) & 0xffL) << 16 //
               | (UNSAFE_ACCESSOR.getByte(address + 3) & 0xffL) << 24 //
               | (UNSAFE_ACCESSOR.getByte(address + 4) & 0xffL) << 32 //
               | (UNSAFE_ACCESSOR.getByte(address + 5) & 0xffL) << 40 //
               | (UNSAFE_ACCESSOR.getByte(address + 6) & 0xffL) << 48 //
               | ((long) UNSAFE_ACCESSOR.getByte(address + 7)) << 56;
    }

    public static void getBytes(long address, byte[] dst, int dstIndex, int length) {
        if (length > JNI_COPY_TO_ARRAY_THRESHOLD) {
            copyMemory(null, address, dst, BYTE_ARRAY_BASE_OFFSET + dstIndex, length);
        } else {
            int end = dstIndex + length;
            for (int i = dstIndex; i < end; i++) {
                dst[i] = UNSAFE_ACCESSOR.getByte(address++);
            }
        }
    }

    public static void setByte(long address, int value) {
        UNSAFE_ACCESSOR.putByte(address, (byte) value);
    }

    public static void setShort(long address, int value) {
        if (UNALIGNED) {
            UNSAFE_ACCESSOR.putShort(address,
                BIG_ENDIAN_NATIVE_ORDER ? (short) value : Short.reverseBytes((short) value));
        } else {
            UNSAFE_ACCESSOR.putByte(address, (byte) (value >>> 8));
            UNSAFE_ACCESSOR.putByte(address + 1, (byte) value);
        }
    }

    public static void setShortLE(long address, int value) {
        if (UNALIGNED) {
            UNSAFE_ACCESSOR.putShort(address, BIG_ENDIAN_NATIVE_ORDER ? Short.reverseBytes((short) value)
                : (short) value);
        } else {
            UNSAFE_ACCESSOR.putByte(address, (byte) value);
            UNSAFE_ACCESSOR.putByte(address + 1, (byte) (value >>> 8));
        }
    }

    public static void setInt(long address, int value) {
        if (UNALIGNED) {
            UNSAFE_ACCESSOR.putInt(address, BIG_ENDIAN_NATIVE_ORDER ? value : Integer.reverseBytes(value));
        } else {
            UNSAFE_ACCESSOR.putByte(address, (byte) (value >>> 24));
            UNSAFE_ACCESSOR.putByte(address + 1, (byte) (value >>> 16));
            UNSAFE_ACCESSOR.putByte(address + 2, (byte) (value >>> 8));
            UNSAFE_ACCESSOR.putByte(address + 3, (byte) value);
        }
    }

    public static void setIntLE(long address, int value) {
        if (UNALIGNED) {
            UNSAFE_ACCESSOR.putInt(address, BIG_ENDIAN_NATIVE_ORDER ? Integer.reverseBytes(value) : value);
        } else {
            UNSAFE_ACCESSOR.putByte(address, (byte) value);
            UNSAFE_ACCESSOR.putByte(address + 1, (byte) (value >>> 8));
            UNSAFE_ACCESSOR.putByte(address + 2, (byte) (value >>> 16));
            UNSAFE_ACCESSOR.putByte(address + 3, (byte) (value >>> 24));
        }
    }

    public static void setLong(long address, long value) {
        if (UNALIGNED) {
            UNSAFE_ACCESSOR.putLong(address, BIG_ENDIAN_NATIVE_ORDER ? value : Long.reverseBytes(value));
        } else {
            UNSAFE_ACCESSOR.putByte(address, (byte) (value >>> 56));
            UNSAFE_ACCESSOR.putByte(address + 1, (byte) (value >>> 48));
            UNSAFE_ACCESSOR.putByte(address + 2, (byte) (value >>> 40));
            UNSAFE_ACCESSOR.putByte(address + 3, (byte) (value >>> 32));
            UNSAFE_ACCESSOR.putByte(address + 4, (byte) (value >>> 24));
            UNSAFE_ACCESSOR.putByte(address + 5, (byte) (value >>> 16));
            UNSAFE_ACCESSOR.putByte(address + 6, (byte) (value >>> 8));
            UNSAFE_ACCESSOR.putByte(address + 7, (byte) value);
        }
    }

    public static void setLongLE(long address, long value) {
        if (UNALIGNED) {
            UNSAFE_ACCESSOR.putLong(address, BIG_ENDIAN_NATIVE_ORDER ? Long.reverseBytes(value) : value);
        } else {
            UNSAFE_ACCESSOR.putByte(address, (byte) value);
            UNSAFE_ACCESSOR.putByte(address + 1, (byte) (value >>> 8));
            UNSAFE_ACCESSOR.putByte(address + 2, (byte) (value >>> 16));
            UNSAFE_ACCESSOR.putByte(address + 3, (byte) (value >>> 24));
            UNSAFE_ACCESSOR.putByte(address + 4, (byte) (value >>> 32));
            UNSAFE_ACCESSOR.putByte(address + 5, (byte) (value >>> 40));
            UNSAFE_ACCESSOR.putByte(address + 6, (byte) (value >>> 48));
            UNSAFE_ACCESSOR.putByte(address + 7, (byte) (value >>> 56));
        }
    }

    public static void setBytes(long address, byte[] src, int srcIndex, int length) {
        if (length > JNI_COPY_FROM_ARRAY_THRESHOLD) {
            copyMemory(src, BYTE_ARRAY_BASE_OFFSET + srcIndex, null, address, length);
        } else {
            int end = srcIndex + length;
            for (int i = srcIndex; i < end; i++) {
                UNSAFE_ACCESSOR.putByte(address++, src[i]);
            }
        }
    }

    private static void copyMemory(Object src, long srcOffset, Object dst, long dstOffset, long length) {
        while (length > 0) {
            long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
            UNSAFE_ACCESSOR.copyMemory(src, srcOffset, dst, dstOffset, size);
            length -= size;
            srcOffset += size;
            dstOffset += size;
        }
    }

    private UnsafeDirectBufferUtil() {
    }
}
