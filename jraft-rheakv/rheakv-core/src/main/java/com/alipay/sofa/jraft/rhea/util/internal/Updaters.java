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

import com.alipay.sofa.jraft.rhea.util.ThrowUtil;
import com.alipay.sofa.jraft.util.internal.UnsafeUtil;

/**
 * Sometime instead of reflection, better performance.
 *
 * @author jiachun.fjc
 */
public class Updaters {

    /**
     * Creates and returns an updater for objects with the given field.
     *
     * @param tClass    the class of the objects holding the field.
     * @param fieldName the name of the field to be updated.
     */
    public static <U> UnsafeIntegerFieldUpdater<U> newIntegerFieldUpdater(final Class<? super U> tClass,
                                                                          final String fieldName) {
        try {
            return new UnsafeIntegerFieldUpdater<>(UnsafeUtil.getUnsafe(), tClass, fieldName);
        } catch (final Throwable t) {
            ThrowUtil.throwException(t);
        }
        return null;
    }

    /**
     * Creates and returns an updater for objects with the given field.
     *
     * @param tClass    the class of the objects holding the field.
     * @param fieldName the name of the field to be updated.
     */
    public static <U> UnsafeLongFieldUpdater<U> newLongFieldUpdater(final Class<? super U> tClass,
                                                                    final String fieldName) {
        try {
            return new UnsafeLongFieldUpdater<>(UnsafeUtil.getUnsafe(), tClass, fieldName);
        } catch (final Throwable t) {
            ThrowUtil.throwException(t);
        }
        return null;
    }

    /**
     * Creates and returns an updater for objects with the given field.
     *
     * @param tClass    the class of the objects holding the field.
     * @param fieldName the name of the field to be updated.
     */
    public static <U, W> UnsafeReferenceFieldUpdater<U, W> newReferenceFieldUpdater(final Class<? super U> tClass,
                                                                                    final String fieldName) {
        try {
            return new UnsafeReferenceFieldUpdater<>(UnsafeUtil.getUnsafe(), tClass, fieldName);
        } catch (final Throwable t) {
            ThrowUtil.throwException(t);
        }
        return null;
    }
}
