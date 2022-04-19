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
package com.alipay.sofa.jraft.rhea.serialization.impl.protostuff.io;

import java.io.IOException;

import com.alipay.sofa.jraft.rhea.serialization.io.OutputBuf;
import com.alipay.sofa.jraft.rhea.util.VarInts;
import com.alipay.sofa.jraft.rhea.util.internal.UnsafeDirectBufferUtil;
import com.alipay.sofa.jraft.util.BufferUtils;
import com.alipay.sofa.jraft.util.internal.UnsafeUtf8Util;
import com.alipay.sofa.jraft.util.internal.UnsafeUtil;

import static io.protostuff.WireFormat.WIRETYPE_LENGTH_DELIMITED;
import static io.protostuff.WireFormat.makeTag;

/**
 *
 * @author jiachun.fjc
 */
class UnsafeNioBufOutput extends NioBufOutput {

    /**
     * Start address of the memory buffer The memory buffer should be non-movable, which normally means that is is allocated
     * off-heap
     */
    private long memoryAddress;

    UnsafeNioBufOutput(OutputBuf outputBuf, int minWritableBytes, int maxCapacity) {
        super(outputBuf, minWritableBytes, maxCapacity);
        updateBufferAddress();
    }

    @Override
    public void writeString(int fieldNumber, CharSequence value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_LENGTH_DELIMITED));

        // UTF-8 byte length of the string is at least its UTF-16 code unit length (value.length()),
        // and at most 3 times of it. We take advantage of this in both branches below.
        int minLength = value.length();
        int maxLength = minLength * UnsafeUtf8Util.MAX_BYTES_PER_CHAR;
        int minLengthVarIntSize = VarInts.computeRawVarInt32Size(minLength);
        int maxLengthVarIntSize = VarInts.computeRawVarInt32Size(maxLength);
        if (minLengthVarIntSize == maxLengthVarIntSize) {
            int position = nioBuffer.position();

            ensureCapacity(maxLengthVarIntSize + maxLength);

            // Save the current position and increment past the length field. We'll come back
            // and write the length field after the encoding is complete.
            int stringStartPos = position + maxLengthVarIntSize;
            BufferUtils.position(nioBuffer, stringStartPos);

            // Encode the string.
            UnsafeUtf8Util.encodeUtf8Direct(value, nioBuffer);

            // Write the length and advance the position.
            int length = nioBuffer.position() - stringStartPos;
            BufferUtils.position(nioBuffer, position);
            writeVarInt32(length);
            BufferUtils.position(nioBuffer, stringStartPos + length);
        } else {
            // Calculate and write the encoded length.
            int length = UnsafeUtf8Util.encodedLength(value);
            writeVarInt32(length);

            ensureCapacity(length);

            // Write the string and advance the position.
            UnsafeUtf8Util.encodeUtf8Direct(value, nioBuffer);
        }
    }

    @Override
    protected void writeVarInt32(int value) throws IOException {
        //        ensureCapacity(5);
        //        int position = nioBuffer.position();
        //        while (true) {
        //            if ((value & ~0x7F) == 0) {
        //                UnsafeDirectBufferUtil.setByte(address(position++), (byte) value);
        //                nioBuffer.position(position);
        //                return;
        //            } else {
        //                UnsafeDirectBufferUtil.setByte(address(position++), (byte) ((value & 0x7F) | 0x80));
        //                value >>>= 7;
        //            }
        //        }
        //
        // The following implementation is no different from the above code function,
        // just aggregate multiple setBytes into a setShort/setInt
        //
        ensureCapacity(5);
        int position = nioBuffer.position();
        if ((value & (~0 << 7)) == 0) {
            // size == 1
            UnsafeDirectBufferUtil.setByte(address(position++), (byte) value);
        } else if ((value & (~0 << 14)) == 0) {
            // size == 2
            UnsafeDirectBufferUtil.setShort(address(position), (((value & 0x7F) | 0x80) << 8) | (value >>> 7));
            position += 2;
        } else if ((value & (~0 << 21)) == 0) {
            // size == 3
            UnsafeDirectBufferUtil.setShort(address(position), (((value & 0x7F) | 0x80) << 8)
                                                               | ((value >>> 7 & 0x7F) | 0x80));
            position += 2;
            UnsafeDirectBufferUtil.setByte(address(position++), (byte) (value >>> 14));
        } else if ((value & (~0 << 28)) == 0) {
            // size == 4
            UnsafeDirectBufferUtil.setInt(address(position), (((value & 0x7F) | 0x80) << 24)
                                                             | (((value >>> 7 & 0x7F) | 0x80) << 16)
                                                             | (((value >>> 14 & 0x7F) | 0x80) << 8) | (value >>> 21));
            position += 4;
        } else {
            // size == 5
            UnsafeDirectBufferUtil.setInt(address(position), (((value & 0x7F) | 0x80) << 24)
                                                             | (((value >>> 7 & 0x7F) | 0x80) << 16)
                                                             | (((value >>> 14 & 0x7F) | 0x80) << 8)
                                                             | ((value >>> 21 & 0x7F) | 0x80));
            position += 4;
            UnsafeDirectBufferUtil.setByte(address(position++), (byte) (value >>> 28));
        }
        BufferUtils.position(nioBuffer, position);
    }

    @Override
    protected void writeVarInt64(long value) throws IOException {
        //        ensureCapacity(10);
        //        int position = nioBuffer.position();
        //        while (true) {
        //            if ((value & ~0x7FL) == 0) {
        //                UnsafeDirectBufferUtil.setByte(address(position++), (byte) value);
        //                nioBuffer.position(position);
        //                return;
        //            } else {
        //                UnsafeDirectBufferUtil.setByte(address(position++), (byte) (((int) value & 0x7F) | 0x80));
        //                value >>>= 7;
        //            }
        //        }
        //
        // The following implementation is no different from the above code function,
        // just aggregate multiple setBytes into a setShort/setInt
        //
        ensureCapacity(10);
        int position = nioBuffer.position();
        // Handle two popular special cases up front ...
        if ((value & (~0L << 7)) == 0) {
            // size == 1
            UnsafeDirectBufferUtil.setByte(address(position++), (byte) value);
        } else if (value < 0L) {
            // size == 10
            UnsafeDirectBufferUtil.setLong(address(position), (((value & 0x7F) | 0x80) << 56)
                                                              | (((value >>> 7 & 0x7F) | 0x80) << 48)
                                                              | (((value >>> 14 & 0x7F) | 0x80) << 40)
                                                              | (((value >>> 21 & 0x7F) | 0x80) << 32)
                                                              | (((value >>> 28 & 0x7F) | 0x80) << 24)
                                                              | (((value >>> 35 & 0x7F) | 0x80) << 16)
                                                              | (((value >>> 42 & 0x7F) | 0x80) << 8)
                                                              | ((value >>> 49 & 0x7F) | 0x80));
            position += 8;
            UnsafeDirectBufferUtil.setShort(address(position), ((((int) (value >>> 56) & 0x7F) | 0x80) << 8)
                                                               | (int) (value >>> 63));
            position += 2;
        }
        // ... leaving us with 8 remaining [2, 3, 4, 5, 6, 7, 8, 9]
        else if ((value & (~0L << 14)) == 0) {
            // size == 2
            UnsafeDirectBufferUtil.setShort(address(position), ((((int) value & 0x7F) | 0x80) << 8)
                                                               | (byte) (value >>> 7));
            position += 2;
        } else if ((value & (~0L << 21)) == 0) {
            // size == 3
            UnsafeDirectBufferUtil.setShort(address(position), ((((int) value & 0x7F) | 0x80) << 8)
                                                               | (((int) value >>> 7 & 0x7F) | 0x80));
            position += 2;
            UnsafeDirectBufferUtil.setByte(address(position++), (byte) (value >>> 14));
        } else if ((value & (~0L << 28)) == 0) {
            // size == 4
            UnsafeDirectBufferUtil.setInt(address(position), ((((int) value & 0x7F) | 0x80) << 24)
                                                             | ((((int) value >>> 7 & 0x7F) | 0x80) << 16)
                                                             | ((((int) value >>> 14 & 0x7F) | 0x80) << 8)
                                                             | ((int) (value >>> 21)));
            position += 4;
        } else if ((value & (~0L << 35)) == 0) {
            // size == 5
            UnsafeDirectBufferUtil.setInt(address(position), ((((int) value & 0x7F) | 0x80) << 24)
                                                             | ((((int) value >>> 7 & 0x7F) | 0x80) << 16)
                                                             | ((((int) value >>> 14 & 0x7F) | 0x80) << 8)
                                                             | (((int) value >>> 21 & 0x7F) | 0x80));
            position += 4;
            UnsafeDirectBufferUtil.setByte(address(position++), (byte) (value >>> 28));
        } else if ((value & (~0L << 42)) == 0) {
            // size == 6
            UnsafeDirectBufferUtil.setInt(address(position), ((((int) value & 0x7F) | 0x80) << 24)
                                                             | ((((int) value >>> 7 & 0x7F) | 0x80) << 16)
                                                             | ((((int) value >>> 14 & 0x7F) | 0x80) << 8)
                                                             | (((int) value >>> 21 & 0x7F) | 0x80));
            position += 4;
            UnsafeDirectBufferUtil.setShort(address(position), ((((int) (value >>> 28) & 0x7F) | 0x80) << 8)
                                                               | (int) (value >>> 35));
            position += 2;
        } else if ((value & (~0L << 49)) == 0) {
            // size == 7
            UnsafeDirectBufferUtil.setInt(address(position), ((((int) value & 0x7F) | 0x80) << 24)
                                                             | ((((int) value >>> 7 & 0x7F) | 0x80) << 16)
                                                             | ((((int) value >>> 14 & 0x7F) | 0x80) << 8)
                                                             | (((int) value >>> 21 & 0x7F) | 0x80));
            position += 4;
            UnsafeDirectBufferUtil.setShort(address(position), ((((int) (value >>> 28) & 0x7F) | 0x80) << 8)
                                                               | (((int) (value >>> 35) & 0x7F) | 0x80));
            position += 2;
            UnsafeDirectBufferUtil.setByte(address(position++), (byte) (value >>> 42));
        } else if ((value & (~0L << 56)) == 0) {
            // size == 8
            UnsafeDirectBufferUtil.setLong(address(position), (((value & 0x7F) | 0x80) << 56)
                                                              | (((value >>> 7 & 0x7F) | 0x80) << 48)
                                                              | (((value >>> 14 & 0x7F) | 0x80) << 40)
                                                              | (((value >>> 21 & 0x7F) | 0x80) << 32)
                                                              | (((value >>> 28 & 0x7F) | 0x80) << 24)
                                                              | (((value >>> 35 & 0x7F) | 0x80) << 16)
                                                              | (((value >>> 42 & 0x7F) | 0x80) << 8) | (value >>> 49));
            position += 8;
        } else {
            // size == 9 (value & (~0L << 63)) == 0
            UnsafeDirectBufferUtil.setLong(address(position), (((value & 0x7F) | 0x80) << 56)
                                                              | (((value >>> 7 & 0x7F) | 0x80) << 48)
                                                              | (((value >>> 14 & 0x7F) | 0x80) << 40)
                                                              | (((value >>> 21 & 0x7F) | 0x80) << 32)
                                                              | (((value >>> 28 & 0x7F) | 0x80) << 24)
                                                              | (((value >>> 35 & 0x7F) | 0x80) << 16)
                                                              | (((value >>> 42 & 0x7F) | 0x80) << 8)
                                                              | ((value >>> 49 & 0x7F) | 0x80));
            position += 8;
            UnsafeDirectBufferUtil.setByte(address(position++), (byte) (value >>> 56));
        }
        BufferUtils.position(nioBuffer, position);
    }

    @Override
    protected void writeInt32LE(int value) throws IOException {
        ensureCapacity(4);
        int position = nioBuffer.position();
        UnsafeDirectBufferUtil.setIntLE(address(position), value);
        BufferUtils.position(nioBuffer, position + 4);
    }

    @Override
    protected void writeInt64LE(long value) throws IOException {
        ensureCapacity(8);
        int position = nioBuffer.position();
        UnsafeDirectBufferUtil.setLongLE(address(position), value);
        BufferUtils.position(nioBuffer, position + 8);
    }

    @Override
    protected void writeByte(byte value) throws IOException {
        ensureCapacity(1);
        int position = nioBuffer.position();
        UnsafeDirectBufferUtil.setByte(address(position), value);
        BufferUtils.position(nioBuffer, position + 1);
    }

    @Override
    protected void writeByteArray(byte[] value, int offset, int length) throws IOException {
        ensureCapacity(length);
        int position = nioBuffer.position();
        UnsafeDirectBufferUtil.setBytes(address(position), value, offset, length);
        BufferUtils.position(nioBuffer, position + length);
    }

    @Override
    protected void ensureCapacity(int required) throws ProtocolException {
        if (nioBuffer.remaining() < required) {
            int position = nioBuffer.position();

            while (capacity - position < required) {
                if (capacity == maxCapacity) {
                    throw new ProtocolException("Buffer overflow. Available: " + (capacity - position) + ", required: "
                                                + required);
                }
                capacity = Math.min(capacity << 1, maxCapacity);
                if (capacity < 0) {
                    capacity = maxCapacity;
                }
            }

            nioBuffer = outputBuf.nioByteBuffer(capacity - position);
            capacity = nioBuffer.limit();
            // Need to update the direct buffer's memory address
            updateBufferAddress();
        }
    }

    private void updateBufferAddress() {
        memoryAddress = UnsafeUtil.addressOffset(nioBuffer);
    }

    private long address(int position) {
        return memoryAddress + position;
    }
}
