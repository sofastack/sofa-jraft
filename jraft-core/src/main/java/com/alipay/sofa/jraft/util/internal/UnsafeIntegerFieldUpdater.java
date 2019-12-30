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
package com.alipay.sofa.jraft.util.internal;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

/**
 *
 * @author jiachun.fjc
 */
final class UnsafeIntegerFieldUpdater<U> implements IntegerFieldUpdater<U> {

    private final long   offset;
    private final Unsafe unsafe;

    UnsafeIntegerFieldUpdater(Unsafe unsafe, Class<? super U> tClass, String fieldName) throws NoSuchFieldException {
        final Field field = tClass.getDeclaredField(fieldName);
        if (unsafe == null) {
            throw new NullPointerException("unsafe");
        }
        this.unsafe = unsafe;
        this.offset = unsafe.objectFieldOffset(field);
    }

    @Override
    public void set(final U obj, final int newValue) {
        this.unsafe.putInt(obj, this.offset, newValue);
    }

    @Override
    public int get(final U obj) {
        return this.unsafe.getInt(obj, this.offset);
    }
}
