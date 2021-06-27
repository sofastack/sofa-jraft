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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/6/27 20:25
 */
public class MmapUtil {
    private static final Logger LOG                     = LoggerFactory.getLogger(MmapUtil.class);

    private static final int    FSYNC_COST_MS_THRESHOLD = 1000;

    public static void unmap(final MappedByteBuffer cb) {
        // JavaSpecVer: 1.6, 1.7, 1.8, 9, 10
        final boolean isOldJDK = System.getProperty("java.specification.version", "99").startsWith("1.");
        try {
            if (isOldJDK) {
                final Method cleaner = cb.getClass().getMethod("cleaner");
                cleaner.setAccessible(true);
                final Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
                clean.setAccessible(true);
                clean.invoke(cleaner.invoke(cb));
            } else {
                Class unsafeClass;
                try {
                    unsafeClass = Class.forName("sun.misc.Unsafe");
                } catch (final Exception ex) {
                    // jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
                    // but that method should be added if sun.misc.Unsafe is removed.
                    unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
                }
                final Method clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
                clean.setAccessible(true);
                final Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
                theUnsafeField.setAccessible(true);
                final Object theUnsafe = theUnsafeField.get(null);
                clean.invoke(theUnsafe, cb);
            }
        } catch (final Exception ex) {
            LOG.error("Fail to un-mapped segment file.", ex);
        }
    }

    public static void fsync(final MappedByteBuffer buffer, String path) {
        if (buffer != null) {
            long startMs = Utils.monotonicMs();
            buffer.force();
            final long cost = Utils.monotonicMs() - startMs;
            if (cost >= FSYNC_COST_MS_THRESHOLD) {
                LOG.warn("Call fsync on file {}  cost {} ms.", path, cost);
            }
        }
    }

    public static void putInt(final MappedByteBuffer buffer, final int index, final int n, final int dataLength) {
        byte[] bs = new byte[dataLength];
        Bits.putInt(bs, 0, n);
        for (int i = 0; i < bs.length; i++) {
            buffer.put(index + i, bs[i]);
        }
    }

    public static void put(final MappedByteBuffer buffer, final int index, final byte[] data) {
        for (int i = 0; i < data.length; i++) {
            buffer.put(index + i, data[i]);
        }
    }
}
