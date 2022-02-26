// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package com.alipay.sofa.jraft.util.internal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import com.alipay.sofa.jraft.util.BufferUtils;

import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_HIGH_SURROGATE;
import static java.lang.Character.MIN_LOW_SURROGATE;
import static java.lang.Character.MIN_SUPPLEMENTARY_CODE_POINT;
import static java.lang.Character.MIN_SURROGATE;
import static java.lang.Character.isSurrogatePair;
import static java.lang.Character.toCodePoint;

/**
 *
 * Refer to the implementation of protobuf: <A>https://github.com/protocolbuffers/protobuf/blob/master/java/core/src/main/java/com/google/protobuf/Utf8.java<A/>.
 */
public final class UnsafeUtf8Util {

    /**
     * Maximum number of bytes per Java UTF-16 char in UTF-8.
     *
     * @see java.nio.charset.CharsetEncoder#maxBytesPerChar()
     */
    public static final int MAX_BYTES_PER_CHAR = 3;

    public static String decodeUtf8(byte[] bytes, int index, int size) {
        if ((index | size | bytes.length - index - size) < 0) {
            throw new ArrayIndexOutOfBoundsException("buffer length=" + bytes.length + ", index=" + index + ", size="
                                                     + size);
        }

        int offset = index;
        final int limit = offset + size;

        // The longest possible resulting String is the same as the number of input bytes, when it is
        // all ASCII. For other cases, this over-allocates and we will truncate in the end.
        char[] resultArr = new char[size];
        int resultPos = 0;

        // Optimize for 100% ASCII (Hotspot loves small simple top-level loops like this).
        // This simple loop stops when we encounter a byte >= 0x80 (i.e. non-ASCII).
        while (offset < limit) {
            byte b = UnsafeUtil.getByte(bytes, offset);
            if (!DecodeUtil.isOneByte(b)) {
                break;
            }
            offset++;
            DecodeUtil.handleOneByte(b, resultArr, resultPos++);
        }

        while (offset < limit) {
            byte byte1 = UnsafeUtil.getByte(bytes, offset++);
            if (DecodeUtil.isOneByte(byte1)) {
                DecodeUtil.handleOneByte(byte1, resultArr, resultPos++);
                // It's common for there to be multiple ASCII characters in a run mixed in, so add an
                // extra optimized loop to take care of these runs.
                while (offset < limit) {
                    byte b = UnsafeUtil.getByte(bytes, offset);
                    if (!DecodeUtil.isOneByte(b)) {
                        break;
                    }
                    offset++;
                    DecodeUtil.handleOneByte(b, resultArr, resultPos++);
                }
            } else if (DecodeUtil.isTwoBytes(byte1)) {
                if (offset >= limit) {
                    throw invalidUtf8();
                }
                DecodeUtil.handleTwoBytes(byte1, /* byte2 */UnsafeUtil.getByte(bytes, offset++), resultArr,
                    resultPos++);
            } else if (DecodeUtil.isThreeBytes(byte1)) {
                if (offset >= limit - 1) {
                    throw invalidUtf8();
                }
                DecodeUtil.handleThreeBytes(byte1,
                /* byte2 */UnsafeUtil.getByte(bytes, offset++),
                /* byte3 */UnsafeUtil.getByte(bytes, offset++), resultArr, resultPos++);
            } else {
                if (offset >= limit - 2) {
                    throw invalidUtf8();
                }
                DecodeUtil.handleFourBytes(byte1,
                /* byte2 */UnsafeUtil.getByte(bytes, offset++),
                /* byte3 */UnsafeUtil.getByte(bytes, offset++),
                /* byte4 */UnsafeUtil.getByte(bytes, offset++), resultArr, resultPos++);
                // 4-byte case requires two chars.
                resultPos++;
            }
        }

        if (resultPos < resultArr.length) {
            resultArr = Arrays.copyOf(resultArr, resultPos);
        }
        return UnsafeUtil.moveToString(resultArr);
    }

    public static String decodeUtf8Direct(ByteBuffer buffer, int index, int size) {
        // Bitwise OR combines the sign bits so any negative value fails the check.
        if ((index | size | buffer.limit() - index - size) < 0) {
            throw new ArrayIndexOutOfBoundsException("buffer limit=" + buffer.limit() + ", index=" + index + ", limit="
                                                     + size);
        }
        long address = UnsafeUtil.addressOffset(buffer) + index;
        final long addressLimit = address + size;

        // The longest possible resulting String is the same as the number of input bytes, when it is
        // all ASCII. For other cases, this over-allocates and we will truncate in the end.
        char[] resultArr = new char[size];
        int resultPos = 0;

        // Optimize for 100% ASCII (Hotspot loves small simple top-level loops like this).
        // This simple loop stops when we encounter a byte >= 0x80 (i.e. non-ASCII).
        while (address < addressLimit) {
            byte b = UnsafeUtil.getByte(address);
            if (!DecodeUtil.isOneByte(b)) {
                break;
            }
            address++;
            DecodeUtil.handleOneByte(b, resultArr, resultPos++);
        }

        while (address < addressLimit) {
            byte byte1 = UnsafeUtil.getByte(address++);
            if (DecodeUtil.isOneByte(byte1)) {
                DecodeUtil.handleOneByte(byte1, resultArr, resultPos++);
                // It's common for there to be multiple ASCII characters in a run mixed in, so add an
                // extra optimized loop to take care of these runs.
                while (address < addressLimit) {
                    byte b = UnsafeUtil.getByte(address);
                    if (!DecodeUtil.isOneByte(b)) {
                        break;
                    }
                    address++;
                    DecodeUtil.handleOneByte(b, resultArr, resultPos++);
                }
            } else if (DecodeUtil.isTwoBytes(byte1)) {
                if (address >= addressLimit) {
                    throw invalidUtf8();
                }
                DecodeUtil.handleTwoBytes(byte1, /* byte2 */UnsafeUtil.getByte(address++), resultArr, resultPos++);
            } else if (DecodeUtil.isThreeBytes(byte1)) {
                if (address >= addressLimit - 1) {
                    throw invalidUtf8();
                }
                DecodeUtil.handleThreeBytes(byte1,
                /* byte2 */UnsafeUtil.getByte(address++),
                /* byte3 */UnsafeUtil.getByte(address++), resultArr, resultPos++);
            } else {
                if (address >= addressLimit - 2) {
                    throw invalidUtf8();
                }
                DecodeUtil.handleFourBytes(byte1,
                /* byte2 */UnsafeUtil.getByte(address++),
                /* byte3 */UnsafeUtil.getByte(address++),
                /* byte4 */UnsafeUtil.getByte(address++), resultArr, resultPos++);
                // 4-byte case requires two chars.
                resultPos++;
            }
        }

        if (resultPos < resultArr.length) {
            resultArr = Arrays.copyOf(resultArr, resultPos);
        }
        return UnsafeUtil.moveToString(resultArr);
    }

    public static int encodeUtf8(CharSequence in, byte[] out, int offset, int length) {
        long outIx = offset;
        final long outLimit = outIx + length;
        final int inLimit = in.length();
        if (inLimit > length || out.length - length < offset) {
            // Not even enough room for an ASCII-encoded string.
            throw new ArrayIndexOutOfBoundsException("Failed writing " + in.charAt(inLimit - 1) + " at index "
                                                     + (offset + length));
        }

        // Designed to take advantage of
        // https://wikis.oracle.com/display/HotSpotInternals/RangeCheckElimination
        int inIx = 0;
        for (char c; inIx < inLimit && (c = in.charAt(inIx)) < 0x80; ++inIx) {
            UnsafeUtil.putByte(out, outIx++, (byte) c);
        }
        if (inIx == inLimit) {
            // We're done, it was ASCII encoded.
            return (int) outIx;
        }

        for (char c; inIx < inLimit; ++inIx) {
            c = in.charAt(inIx);
            if (c < 0x80 && outIx < outLimit) {
                UnsafeUtil.putByte(out, outIx++, (byte) c);
            } else if (c < 0x800 && outIx <= outLimit - 2L) { // 11 bits, two UTF-8 bytes
                UnsafeUtil.putByte(out, outIx++, (byte) ((0xF << 6) | (c >>> 6)));
                UnsafeUtil.putByte(out, outIx++, (byte) (0x80 | (0x3F & c)));
            } else if ((c < MIN_SURROGATE || MAX_SURROGATE < c) && outIx <= outLimit - 3L) {
                // Maximum single-char code point is 0xFFFF, 16 bits, three UTF-8 bytes
                UnsafeUtil.putByte(out, outIx++, (byte) ((0xF << 5) | (c >>> 12)));
                UnsafeUtil.putByte(out, outIx++, (byte) (0x80 | (0x3F & (c >>> 6))));
                UnsafeUtil.putByte(out, outIx++, (byte) (0x80 | (0x3F & c)));
            } else if (outIx <= outLimit - 4L) {
                // Minimum code point represented by a surrogate pair is 0x10000, 17 bits, four UTF-8
                // bytes
                final char low;
                if (inIx + 1 == inLimit || !isSurrogatePair(c, (low = in.charAt(++inIx)))) {
                    throw new IllegalArgumentException("Unpaired surrogate at index " + (inIx - 1) + " of " + inLimit);
                }
                int codePoint = toCodePoint(c, low);
                UnsafeUtil.putByte(out, outIx++, (byte) ((0xF << 4) | (codePoint >>> 18)));
                UnsafeUtil.putByte(out, outIx++, (byte) (0x80 | (0x3F & (codePoint >>> 12))));
                UnsafeUtil.putByte(out, outIx++, (byte) (0x80 | (0x3F & (codePoint >>> 6))));
                UnsafeUtil.putByte(out, outIx++, (byte) (0x80 | (0x3F & codePoint)));
            } else {
                if ((MIN_SURROGATE <= c && c <= MAX_SURROGATE)
                    && (inIx + 1 == inLimit || !isSurrogatePair(c, in.charAt(inIx + 1)))) {
                    // We are surrogates and we're not a surrogate pair.
                    throw new IllegalArgumentException("Unpaired surrogate at index " + inIx + " of " + inLimit);
                }
                // Not enough space in the output buffer.
                throw new ArrayIndexOutOfBoundsException("Failed writing " + c + " at index " + outIx);
            }
        }

        // All bytes have been encoded.
        return (int) outIx;
    }

    public static void encodeUtf8Direct(CharSequence in, ByteBuffer out) {
        final long address = UnsafeUtil.addressOffset(out);
        long outIx = address + out.position();
        final long outLimit = address + out.limit();
        final int inLimit = in.length();
        if (inLimit > outLimit - outIx) {
            // Not even enough room for an ASCII-encoded string.
            throw new ArrayIndexOutOfBoundsException("Failed writing " + in.charAt(inLimit - 1) + " at index "
                                                     + out.limit());
        }

        // Designed to take advantage of
        // https://wikis.oracle.com/display/HotSpotInternals/RangeCheckElimination
        int inIx = 0;
        for (char c; inIx < inLimit && (c = in.charAt(inIx)) < 0x80; ++inIx) {
            UnsafeUtil.putByte(outIx++, (byte) c);
        }
        if (inIx == inLimit) {
            // We're done, it was ASCII encoded.
            BufferUtils.position(out, (int) (outIx - address));
            return;
        }

        for (char c; inIx < inLimit; ++inIx) {
            c = in.charAt(inIx);
            if (c < 0x80 && outIx < outLimit) {
                UnsafeUtil.putByte(outIx++, (byte) c);
            } else if (c < 0x800 && outIx <= outLimit - 2L) { // 11 bits, two UTF-8 bytes
                UnsafeUtil.putByte(outIx++, (byte) ((0xF << 6) | (c >>> 6)));
                UnsafeUtil.putByte(outIx++, (byte) (0x80 | (0x3F & c)));
            } else if ((c < MIN_SURROGATE || MAX_SURROGATE < c) && outIx <= outLimit - 3L) {
                // Maximum single-char code point is 0xFFFF, 16 bits, three UTF-8 bytes
                UnsafeUtil.putByte(outIx++, (byte) ((0xF << 5) | (c >>> 12)));
                UnsafeUtil.putByte(outIx++, (byte) (0x80 | (0x3F & (c >>> 6))));
                UnsafeUtil.putByte(outIx++, (byte) (0x80 | (0x3F & c)));
            } else if (outIx <= outLimit - 4L) {
                // Minimum code point represented by a surrogate pair is 0x10000, 17 bits, four UTF-8
                // bytes
                final char low;
                if (inIx + 1 == inLimit || !isSurrogatePair(c, (low = in.charAt(++inIx)))) {
                    throw new IllegalArgumentException("Unpaired surrogate at index " + (inIx - 1) + " of " + inLimit);
                }
                int codePoint = toCodePoint(c, low);
                UnsafeUtil.putByte(outIx++, (byte) ((0xF << 4) | (codePoint >>> 18)));
                UnsafeUtil.putByte(outIx++, (byte) (0x80 | (0x3F & (codePoint >>> 12))));
                UnsafeUtil.putByte(outIx++, (byte) (0x80 | (0x3F & (codePoint >>> 6))));
                UnsafeUtil.putByte(outIx++, (byte) (0x80 | (0x3F & codePoint)));
            } else {
                if ((MIN_SURROGATE <= c && c <= MAX_SURROGATE)
                    && (inIx + 1 == inLimit || !isSurrogatePair(c, in.charAt(inIx + 1)))) {
                    // We are surrogates and we're not a surrogate pair.
                    throw new IllegalArgumentException("Unpaired surrogate at index " + inIx + " of " + inLimit);
                }
                // Not enough space in the output buffer.
                throw new ArrayIndexOutOfBoundsException("Failed writing " + c + " at index " + outIx);
            }
        }

        // All bytes have been encoded.
        BufferUtils.position(out, (int) (outIx - address));
    }

    /**
     * Returns the number of bytes in the UTF-8-encoded form of {@code sequence}. For a string,
     * this method is equivalent to {@code string.getBytes(UTF_8).length}, but is more efficient in
     * both time and space.
     *
     * @throws IllegalArgumentException if {@code sequence} contains ill-formed UTF-16 (unpaired
     *                                  surrogates)
     */
    public static int encodedLength(CharSequence sequence) {
        // Warning to maintainers: this implementation is highly optimized.
        int utf16Length = sequence.length();
        int utf8Length = utf16Length;
        int i = 0;

        // This loop optimizes for pure ASCII.
        while (i < utf16Length && sequence.charAt(i) < 0x80) {
            i++;
        }

        // This loop optimizes for chars less than 0x800.
        for (; i < utf16Length; i++) {
            char c = sequence.charAt(i);
            if (c < 0x800) {
                utf8Length += ((0x7f - c) >>> 31); // branch free!
            } else {
                utf8Length += encodedLengthGeneral(sequence, i);
                break;
            }
        }

        if (utf8Length < utf16Length) {
            // Necessary and sufficient condition for overflow because of maximum 3x expansion
            throw new IllegalArgumentException("UTF-8 length does not fit in int: " + (utf8Length + (1L << 32)));
        }
        return utf8Length;
    }

    private static int encodedLengthGeneral(CharSequence sequence, int start) {
        int utf16Length = sequence.length();
        int utf8Length = 0;
        for (int i = start; i < utf16Length; i++) {
            char c = sequence.charAt(i);
            if (c < 0x800) {
                utf8Length += (0x7f - c) >>> 31; // branch free!
            } else {
                utf8Length += 2;
                // jdk7+: if (Character.isSurrogate(c)) {
                if (Character.MIN_SURROGATE <= c && c <= Character.MAX_SURROGATE) {
                    // Check that we have a well-formed surrogate pair.
                    int cp = Character.codePointAt(sequence, i);
                    if (cp < MIN_SUPPLEMENTARY_CODE_POINT) {
                        throw new IllegalArgumentException("Unpaired surrogate at index " + i + " of " + utf16Length);
                    }
                    i++;
                }
            }
        }
        return utf8Length;
    }

    /**
     * Utility methods for decoding bytes into {@link String}. Callers are responsible for extracting
     * bytes (possibly using Unsafe methods), and checking remaining bytes. All other UTF-8 validity
     * checks and codepoint conversion happen in this class.
     */
    private static class DecodeUtil {

        /**
         * Returns whether this is a single-byte codepoint (i.e., ASCII) with the form '0XXXXXXX'.
         */
        private static boolean isOneByte(byte b) {
            return b >= 0;
        }

        /**
         * Returns whether this is a two-byte codepoint with the form '10XXXXXX'.
         */
        private static boolean isTwoBytes(byte b) {
            return b < (byte) 0xE0;
        }

        /**
         * Returns whether this is a three-byte codepoint with the form '110XXXXX'.
         */
        private static boolean isThreeBytes(byte b) {
            return b < (byte) 0xF0;
        }

        private static void handleOneByte(byte byte1, char[] resultArr, int resultPos) {
            resultArr[resultPos] = (char) byte1;
        }

        private static void handleTwoBytes(byte byte1, byte byte2, char[] resultArr, int resultPos) {
            // Simultaneously checks for illegal trailing-byte in leading position (<= '11000000') and
            // overlong 2-byte, '11000001'.
            if (byte1 < (byte) 0xC2 || isNotTrailingByte(byte2)) {
                throw invalidUtf8();
            }
            resultArr[resultPos] = (char) (((byte1 & 0x1F) << 6) | trailingByteValue(byte2));
        }

        private static void handleThreeBytes(byte byte1, byte byte2, byte byte3, char[] resultArr, int resultPos) {
            if (isNotTrailingByte(byte2)
            // overlong? 5 most significant bits must not all be zero
                || (byte1 == (byte) 0xE0 && byte2 < (byte) 0xA0)
                // check for illegal surrogate codepoints
                || (byte1 == (byte) 0xED && byte2 >= (byte) 0xA0) || isNotTrailingByte(byte3)) {
                throw invalidUtf8();
            }
            resultArr[resultPos] = (char) (((byte1 & 0x0F) << 12) | (trailingByteValue(byte2) << 6) | trailingByteValue(byte3));
        }

        private static void handleFourBytes(byte byte1, byte byte2, byte byte3, byte byte4, char[] resultArr,
                                            int resultPos) {
            if (isNotTrailingByte(byte2)
                // Check that 1 <= plane <= 16.  Tricky optimized form of:
                //   valid 4-byte leading byte?
                // if (byte1 > (byte) 0xF4 ||
                //   overlong? 4 most significant bits must not all be zero
                //     byte1 == (byte) 0xF0 && byte2 < (byte) 0x90 ||
                //   codepoint larger than the highest code point (U+10FFFF)?
                //     byte1 == (byte) 0xF4 && byte2 > (byte) 0x8F)
                || (((byte1 << 28) + (byte2 - (byte) 0x90)) >> 30) != 0 || isNotTrailingByte(byte3)
                || isNotTrailingByte(byte4)) {
                throw invalidUtf8();
            }
            int codePoint = ((byte1 & 0x07) << 18) | (trailingByteValue(byte2) << 12) | (trailingByteValue(byte3) << 6)
                            | trailingByteValue(byte4);
            resultArr[resultPos] = DecodeUtil.highSurrogate(codePoint);
            resultArr[resultPos + 1] = DecodeUtil.lowSurrogate(codePoint);
        }

        /**
         * Returns whether the byte is not a valid continuation of the form '10XXXXXX'.
         */
        private static boolean isNotTrailingByte(byte b) {
            return b > (byte) 0xBF;
        }

        /**
         * Returns the actual value of the trailing byte (removes the prefix '10') for composition.
         */
        private static int trailingByteValue(byte b) {
            return b & 0x3F;
        }

        private static char highSurrogate(int codePoint) {
            return (char) ((MIN_HIGH_SURROGATE - (MIN_SUPPLEMENTARY_CODE_POINT >>> 10)) + (codePoint >>> 10));
        }

        private static char lowSurrogate(int codePoint) {
            return (char) (MIN_LOW_SURROGATE + (codePoint & 0x3ff));
        }
    }

    static IllegalStateException invalidUtf8() {
        return new IllegalStateException("Message had invalid UTF-8.");
    }

    private UnsafeUtf8Util() {
    }
}
