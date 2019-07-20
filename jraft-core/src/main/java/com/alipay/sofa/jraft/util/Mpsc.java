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

import java.util.Queue;

import org.jctools.queues.MpscChunkedArrayQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.jctools.queues.atomic.MpscGrowableAtomicArrayQueue;
import org.jctools.queues.atomic.MpscUnboundedAtomicArrayQueue;

import com.alipay.sofa.jraft.util.internal.UnsafeUtil;

/**
 * @author jiachun.fjc
 */
public final class Mpsc {

    private static final int MPSC_CHUNK_SIZE       = 1024;
    private static final int MIN_MAX_MPSC_CAPACITY = MPSC_CHUNK_SIZE << 1;

    public static Queue<Runnable> newMpscQueue() {
        return UnsafeUtil.hasUnsafe() ? new MpscUnboundedArrayQueue<>(MPSC_CHUNK_SIZE)
            : new MpscUnboundedAtomicArrayQueue<>(MPSC_CHUNK_SIZE);
    }

    public static Queue<Runnable> newMpscQueue(final int maxCapacity) {
        final int capacity = Math.max(MIN_MAX_MPSC_CAPACITY, maxCapacity);
        return UnsafeUtil.hasUnsafe() ? new MpscChunkedArrayQueue<>(MPSC_CHUNK_SIZE, capacity)
            : new MpscGrowableAtomicArrayQueue<>(MPSC_CHUNK_SIZE, capacity);
    }
}
