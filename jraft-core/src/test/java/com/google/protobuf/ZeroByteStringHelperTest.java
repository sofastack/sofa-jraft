package com.google.protobuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class ZeroByteStringHelperTest {

    @SuppressWarnings("ConstantConditions")
    @Test
    public void concatenateTest() {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final List<ByteString> byteStrings = new ArrayList<>();
        final List<ByteBuffer> byteBuffers = new ArrayList<>();
        final int segSize = 512;

        int start;
        int end = 0;
        byte[] bytes;

        for (int j = 0; j < 100; j++) {
            start = end;
            end = random.nextInt(start, start + segSize);
            bytes = new byte[end];
            for (int i = start; i < end; i++) {
                bytes[i - start] = (byte) i;
            }
            byteBuffers.add(ByteBuffer.wrap(bytes));
            byteStrings.add(ZeroByteStringHelper.wrap(bytes));
        }
        final ByteString all = ZeroByteStringHelper.concatenate(byteBuffers);

        int i = 0;
        for (final ByteString bs : byteStrings) {
            for (Byte b : bs) {
                Assert.assertEquals(b.byteValue(), all.byteAt(i++));
            }
        }
        System.out.println(i);
    }
}
