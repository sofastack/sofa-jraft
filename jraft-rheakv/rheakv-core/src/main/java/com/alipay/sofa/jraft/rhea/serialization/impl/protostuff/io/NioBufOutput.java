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
import java.nio.ByteBuffer;

import com.alipay.sofa.jraft.util.BufferUtils;
import io.protostuff.ByteString;
import io.protostuff.IntSerializer;
import io.protostuff.Output;
import io.protostuff.Schema;

import com.alipay.sofa.jraft.rhea.serialization.io.OutputBuf;
import com.alipay.sofa.jraft.rhea.util.VarInts;
import com.alipay.sofa.jraft.util.internal.ReferenceFieldUpdater;
import com.alipay.sofa.jraft.util.internal.UnsafeUtf8Util;
import com.alipay.sofa.jraft.util.internal.Updaters;
import static io.protostuff.ProtobufOutput.encodeZigZag32;
import static io.protostuff.ProtobufOutput.encodeZigZag64;
import static io.protostuff.WireFormat.WIRETYPE_END_GROUP;
import static io.protostuff.WireFormat.WIRETYPE_FIXED32;
import static io.protostuff.WireFormat.WIRETYPE_FIXED64;
import static io.protostuff.WireFormat.WIRETYPE_LENGTH_DELIMITED;
import static io.protostuff.WireFormat.WIRETYPE_START_GROUP;
import static io.protostuff.WireFormat.WIRETYPE_VARINT;
import static io.protostuff.WireFormat.makeTag;

/**
 *
 * @author jiachun.fjc
 */
class NioBufOutput implements Output {

    private static final ReferenceFieldUpdater<ByteString, byte[]> byteStringBytesGetter = Updaters
                                                                                             .newReferenceFieldUpdater(
                                                                                                 ByteString.class,
                                                                                                 "bytes");

    protected final OutputBuf                                      outputBuf;
    protected final int                                            maxCapacity;
    protected ByteBuffer                                           nioBuffer;
    protected int                                                  capacity;

    NioBufOutput(OutputBuf outputBuf, int minWritableBytes, int maxCapacity) {
        this.outputBuf = outputBuf;
        this.maxCapacity = maxCapacity;
        nioBuffer = outputBuf.nioByteBuffer(minWritableBytes);
        capacity = nioBuffer.remaining();
    }

    @Override
    public void writeInt32(int fieldNumber, int value, boolean repeated) throws IOException {
        if (value < 0) {
            writeVarInt32(makeTag(fieldNumber, WIRETYPE_VARINT));
            writeVarInt64(value);
        } else {
            writeVarInt32(makeTag(fieldNumber, WIRETYPE_VARINT));
            writeVarInt32(value);
        }
    }

    @Override
    public void writeUInt32(int fieldNumber, int value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_VARINT));
        writeVarInt32(value);
    }

    @Override
    public void writeSInt32(int fieldNumber, int value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_VARINT));
        writeVarInt32(encodeZigZag32(value));
    }

    @Override
    public void writeFixed32(int fieldNumber, int value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_FIXED32));
        writeInt32LE(value);
    }

    @Override
    public void writeSFixed32(int fieldNumber, int value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_FIXED32));
        writeInt32LE(value);
    }

    @Override
    public void writeInt64(int fieldNumber, long value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_VARINT));
        writeVarInt64(value);
    }

    @Override
    public void writeUInt64(int fieldNumber, long value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_VARINT));
        writeVarInt64(value);
    }

    @Override
    public void writeSInt64(int fieldNumber, long value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_VARINT));
        writeVarInt64(encodeZigZag64(value));
    }

    @Override
    public void writeFixed64(int fieldNumber, long value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_FIXED64));
        writeInt64LE(value);
    }

    @Override
    public void writeSFixed64(int fieldNumber, long value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_FIXED64));
        writeInt64LE(value);
    }

    @Override
    public void writeFloat(int fieldNumber, float value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_FIXED32));
        writeInt32LE(Float.floatToRawIntBits(value));
    }

    @Override
    public void writeDouble(int fieldNumber, double value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_FIXED64));
        writeInt64LE(Double.doubleToRawLongBits(value));
    }

    @Override
    public void writeBool(int fieldNumber, boolean value, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_VARINT));
        writeByte(value ? (byte) 0x01 : 0x00);
    }

    @Override
    public void writeEnum(int fieldNumber, int value, boolean repeated) throws IOException {
        writeInt32(fieldNumber, value, repeated);
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

            int length;
            // Encode the string.
            if (nioBuffer.isDirect()) {
                UnsafeUtf8Util.encodeUtf8Direct(value, nioBuffer);
                // Write the length and advance the position.
                length = nioBuffer.position() - stringStartPos;
            } else {
                int offset = nioBuffer.arrayOffset() + stringStartPos;
                int outIndex = UnsafeUtf8Util.encodeUtf8(value, nioBuffer.array(), offset, nioBuffer.remaining());
                length = outIndex - offset;
            }
            BufferUtils.position(nioBuffer, position);
            writeVarInt32(length);
            BufferUtils.position(nioBuffer, stringStartPos + length);
        } else {
            // Calculate and write the encoded length.
            int length = UnsafeUtf8Util.encodedLength(value);
            writeVarInt32(length);

            ensureCapacity(length);

            if (nioBuffer.isDirect()) {
                // Write the string and advance the position.
                UnsafeUtf8Util.encodeUtf8Direct(value, nioBuffer);
            } else {
                int pos = nioBuffer.position();
                UnsafeUtf8Util.encodeUtf8(value, nioBuffer.array(), nioBuffer.arrayOffset() + pos,
                    nioBuffer.remaining());
                BufferUtils.position(nioBuffer, pos + length);
            }
        }
    }

    @Override
    public void writeBytes(int fieldNumber, ByteString value, boolean repeated) throws IOException {
        writeByteArray(fieldNumber, byteStringBytesGetter.get(value), repeated);
    }

    @Override
    public void writeByteArray(int fieldNumber, byte[] value, boolean repeated) throws IOException {
        writeByteRange(false, fieldNumber, value, 0, value.length, repeated);
    }

    @Override
    public void writeByteRange(boolean utf8String, int fieldNumber, byte[] value, int offset, int length,
                               boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_LENGTH_DELIMITED));
        writeVarInt32(length);
        writeByteArray(value, offset, length);
    }

    @Override
    public <T> void writeObject(int fieldNumber, T value, Schema<T> schema, boolean repeated) throws IOException {
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_START_GROUP));
        schema.writeTo(this, value);
        writeVarInt32(makeTag(fieldNumber, WIRETYPE_END_GROUP));
    }

    @Override
    public void writeBytes(int fieldNumber, ByteBuffer value, boolean repeated) throws IOException {
        writeByteRange(false, fieldNumber, value.array(), value.arrayOffset() + value.position(), value.remaining(),
            repeated);
    }

    protected void writeVarInt32(int value) throws IOException {
        ensureCapacity(5);
        while (true) {
            if ((value & ~0x7F) == 0) {
                nioBuffer.put((byte) value);
                return;
            } else {
                nioBuffer.put((byte) ((value & 0x7F) | 0x80));
                value >>>= 7;
            }
        }
    }

    protected void writeVarInt64(long value) throws IOException {
        ensureCapacity(10);
        while (true) {
            if ((value & ~0x7FL) == 0) {
                nioBuffer.put((byte) value);
                return;
            } else {
                nioBuffer.put((byte) (((int) value & 0x7F) | 0x80));
                value >>>= 7;
            }
        }
    }

    protected void writeInt32LE(final int value) throws IOException {
        ensureCapacity(4);
        IntSerializer.writeInt32LE(value, nioBuffer);
    }

    protected void writeInt64LE(final long value) throws IOException {
        ensureCapacity(8);
        IntSerializer.writeInt64LE(value, nioBuffer);
    }

    protected void writeByte(final byte value) throws IOException {
        ensureCapacity(1);
        nioBuffer.put(value);
    }

    protected void writeByteArray(final byte[] value, final int offset, final int length) throws IOException {
        ensureCapacity(length);
        nioBuffer.put(value, offset, length);
    }

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
        }
    }
}
