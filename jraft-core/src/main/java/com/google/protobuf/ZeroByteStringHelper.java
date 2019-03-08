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
package com.google.protobuf;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Byte string and byte buffer converter
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-May-08 2:38:42 PM
 */
public class ZeroByteStringHelper {

    private static MethodHandle WRAP_BUF_HANDLE   = null;
    private static MethodHandle WRAP_BYTES_HANDLE = null;

    static {
        // Try to get defineAnonymousClass method handle.
        try {
            final Class<?> clazz = ByteString.class;
            final Method method = clazz.getDeclaredMethod("wrap", ByteBuffer.class);
            if (method != null) {
                WRAP_BUF_HANDLE = MethodHandles.lookup().unreflect(method);
            }

        } catch (final Throwable ignored) {
            // ignored
        }
        // Try to get defineAnonymousClass method handle.
        try {
            final Class<?> clazz = ByteString.class;
            final Method method = clazz.getDeclaredMethod("wrap", byte[].class);
            if (method != null) {
                WRAP_BYTES_HANDLE = MethodHandles.lookup().unreflect(method);
            }

        } catch (final Throwable ignored) {
            // ignored
        }
    }

    /**
     * Wrap a byte array into a ByteString.
     */
    public static ByteString wrap(byte[] bs) {
        if (WRAP_BYTES_HANDLE != null) {
            try {
                return (ByteString) WRAP_BYTES_HANDLE.invoke(bs);
            } catch (final Throwable ignored) {
                // ignored
            }
        }
        return ByteString.copyFrom(bs);
    }

    /**
     * Wrap a byte buffer into a ByteString.
     */
    public static ByteString wrap(ByteBuffer buf) {
        if (WRAP_BUF_HANDLE != null) {
            try {
                return (ByteString) WRAP_BUF_HANDLE.invoke(buf);
            } catch (final Throwable ignored) {
                // ignored
            }
        }
        return ByteString.copyFrom(buf);
    }
}
