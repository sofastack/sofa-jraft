/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.alipay.sofa.jraft.util;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Light-weight object pool based on a thread-local stack.
 * <p/>
 * Forked from <a href="https://github.com/netty/netty">Netty</a> 4.1.69+ with the following
 * improvements synced from Netty:
 * <ul>
 *   <li>Fix reclaimSpace bug in availableSharedCapacity (Netty #9394)</li>
 *   <li>Avoid recycling to terminated threads (Netty #11996)</li>
 *   <li>Use CAS for lastRecycledId to handle racing recycle calls from multiple threads</li>
 *   <li>Add recycling ratio to prevent aggressive capacity growth</li>
 *   <li>Add max delayed queues per thread to bound memory usage</li>
 *   <li>Better cleanup on WeakOrderQueue removal</li>
 * </ul>
 *
 * @param <T> the type of the pooled object
 */
public abstract class Recyclers<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Recyclers.class);

    @SuppressWarnings("rawtypes")
    public static final Handle NOOP_HANDLE = new Handle() {};

    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(Integer.MIN_VALUE);
    private static final int OWN_THREAD_ID = ID_GENERATOR.getAndIncrement();
    private static final int DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD = 4 * 1024;
    private static final int DEFAULT_MAX_CAPACITY_PER_THREAD;
    private static final int INITIAL_CAPACITY;
    private static final int MAX_SHARED_CAPACITY_FACTOR;
    private static final int MAX_DELAYED_QUEUES_PER_THREAD;
    private static final int LINK_CAPACITY;
    private static final int RATIO;
    private static final int DELAYED_QUEUE_RATIO;

    static {
        int maxCapacityPerThread = SystemPropertyUtil.getInt(
                "jraft.recyclers.maxCapacityPerThread",
                SystemPropertyUtil.getInt("jraft.recyclers.maxCapacity", DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD));
        if (maxCapacityPerThread < 0) {
            maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
        }
        DEFAULT_MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;

        MAX_SHARED_CAPACITY_FACTOR = max(2,
                SystemPropertyUtil.getInt("jraft.recyclers.maxSharedCapacityFactor", 2));

        MAX_DELAYED_QUEUES_PER_THREAD = max(0,
                SystemPropertyUtil.getInt("jraft.recyclers.maxDelayedQueuesPerThread",
                        Runtime.getRuntime().availableProcessors() * 2));

        LINK_CAPACITY = safeFindNextPositivePowerOfTwo(
                max(SystemPropertyUtil.getInt("jraft.recyclers.linkCapacity", 16), 16));

        // By default we allow one push to a Recycler for each 8th try on handles that were never recycled before.
        RATIO = max(0, SystemPropertyUtil.getInt("jraft.recyclers.ratio", 8));
        DELAYED_QUEUE_RATIO = max(0, SystemPropertyUtil.getInt("jraft.recyclers.delayedQueueRatio", RATIO));

        INITIAL_CAPACITY = Math.min(DEFAULT_MAX_CAPACITY_PER_THREAD, 256);

        LOG.info("-Djraft.recyclers.maxCapacityPerThread: {}.",
                DEFAULT_MAX_CAPACITY_PER_THREAD == 0 ? "disabled" : DEFAULT_MAX_CAPACITY_PER_THREAD);
        LOG.info("-Djraft.recyclers.maxSharedCapacityFactor: {}.", MAX_SHARED_CAPACITY_FACTOR);
        LOG.info("-Djraft.recyclers.linkCapacity: {}.", LINK_CAPACITY);
        LOG.info("-Djraft.recyclers.ratio: {}.", RATIO);
        LOG.info("-Djraft.recyclers.delayedQueueRatio: {}.", DELAYED_QUEUE_RATIO);
    }

    private static int max(int a, int b) {
        return a >= b ? a : b;
    }

    private static int safeFindNextPositivePowerOfTwo(int value) {
        return value <= 0 || value >= 1073741824 ? 1073741824 : Integer.highestOneBit(value) << 1;
    }

    private final int maxCapacityPerThread;
    private final int maxSharedCapacityFactor;
    private final int interval;
    private final int maxDelayedQueuesPerThread;
    private final int delayedQueueInterval;

    private final ThreadLocal<Stack<T>> threadLocal = new ThreadLocal<Stack<T>>() {
        @Override
        protected Stack<T> initialValue() {
            return new Stack<>(Recyclers.this, Thread.currentThread(), maxCapacityPerThread,
                    maxSharedCapacityFactor, interval, maxDelayedQueuesPerThread, delayedQueueInterval);
        }
    };

    protected Recyclers() {
        this(DEFAULT_MAX_CAPACITY_PER_THREAD);
    }

    protected Recyclers(int maxCapacityPerThread) {
        this(maxCapacityPerThread, MAX_SHARED_CAPACITY_FACTOR);
    }

    protected Recyclers(int maxCapacityPerThread, int maxSharedCapacityFactor) {
        this(maxCapacityPerThread, maxSharedCapacityFactor, RATIO, MAX_DELAYED_QUEUES_PER_THREAD);
    }

    protected Recyclers(int maxCapacityPerThread, int maxSharedCapacityFactor,
                        int ratio, int maxDelayedQueuesPerThread) {
        this(maxCapacityPerThread, maxSharedCapacityFactor, ratio, maxDelayedQueuesPerThread, DELAYED_QUEUE_RATIO);
    }

    protected Recyclers(int maxCapacityPerThread, int maxSharedCapacityFactor,
                        int ratio, int maxDelayedQueuesPerThread, int delayedQueueRatio) {
        this.interval = max(0, ratio);
        this.delayedQueueInterval = max(0, delayedQueueRatio);
        if (maxCapacityPerThread <= 0) {
            this.maxCapacityPerThread = 0;
            this.maxSharedCapacityFactor = 1;
            this.maxDelayedQueuesPerThread = 0;
        } else {
            this.maxCapacityPerThread = maxCapacityPerThread;
            this.maxSharedCapacityFactor = max(1, maxSharedCapacityFactor);
            this.maxDelayedQueuesPerThread = max(0, maxDelayedQueuesPerThread);
        }
    }

    @SuppressWarnings("unchecked")
    public final T get() {
        if (maxCapacityPerThread == 0) {
            return newObject(NOOP_HANDLE);
        }
        Stack<T> stack = threadLocal.get();
        DefaultHandle<T> handle = stack.pop();
        if (handle == null) {
            handle = stack.newHandle();
            handle.value = newObject(handle);
        }
        return (T) handle.value;
    }

    /**
     * @deprecated use {@link Handle#recycle(Object)}.
     */
    @Deprecated
    public final boolean recycle(T o, Handle handle) {
        if (handle == NOOP_HANDLE) {
            return false;
        }

        DefaultHandle<T> h = (DefaultHandle<T>) handle;
        if (h.stack.parent != this) {
            return false;
        }

        h.recycle(o);
        return true;
    }

    protected abstract T newObject(Handle handle);

    public final int threadLocalCapacity() {
        return threadLocal.get().elements.length;
    }

    public final int threadLocalSize() {
        return threadLocal.get().size;
    }

    public interface Handle {
        default void recycle(Object object) {}
    }

    @SuppressWarnings("unchecked")
    private static final class DefaultHandle<T> implements Handle {
        private static final AtomicIntegerFieldUpdater<DefaultHandle<?>> LAST_RECYCLED_ID_UPDATER;

        static {
            AtomicIntegerFieldUpdater<?> updater = AtomicIntegerFieldUpdater.newUpdater(
                    DefaultHandle.class, "lastRecycledId");
            LAST_RECYCLED_ID_UPDATER = (AtomicIntegerFieldUpdater<DefaultHandle<?>>) updater;
        }

        volatile int lastRecycledId;
        int recycleId;

        boolean hasBeenRecycled;

        Stack<?> stack;
        Object value;

        DefaultHandle(Stack<?> stack) {
            this.stack = stack;
        }

        @Override
        public void recycle(Object object) {
            if (object != value) {
                throw new IllegalArgumentException("object does not belong to handle");
            }
            Stack<?> stack = this.stack;
            if (lastRecycledId != recycleId || stack == null) {
                throw new IllegalStateException("recycled already");
            }
            stack.push(this);
        }

        public boolean compareAndSetLastRecycledId(int expectLastRecycledId, int updateLastRecycledId) {
            return LAST_RECYCLED_ID_UPDATER.weakCompareAndSet(this, expectLastRecycledId, updateLastRecycledId);
        }
    }

    private static final ThreadLocal<Map<Stack<?>, WeakOrderQueue>> DELAYED_RECYCLED =
            ThreadLocal.withInitial(WeakHashMap::new);

    private static final class WeakOrderQueue extends WeakReference<Thread> {

        static final WeakOrderQueue DUMMY = new WeakOrderQueue();

        @SuppressWarnings("serial")
        static final class Link extends AtomicInteger {
            final DefaultHandle<?>[] elements = new DefaultHandle[LINK_CAPACITY];
            int readIndex;
            Link next;
        }

        private static final class Head {
            private final AtomicInteger availableSharedCapacity;

            Link link;

            Head(AtomicInteger availableSharedCapacity) {
                this.availableSharedCapacity = availableSharedCapacity;
            }

            void reclaimAllSpaceAndUnlink() {
                Link head = link;
                link = null;
                int reclaimSpace = 0;
                while (head != null) {
                    reclaimSpace += LINK_CAPACITY;
                    Link next = head.next;
                    head.next = null;
                    head = next;
                }
                if (reclaimSpace > 0) {
                    reclaimSpace(reclaimSpace);
                }
            }

            private void reclaimSpace(int space) {
                availableSharedCapacity.addAndGet(space);
            }

            void relink(Link link) {
                reclaimSpace(LINK_CAPACITY);
                this.link = link;
            }

            Link newLink() {
                return reserveSpaceForLink(availableSharedCapacity) ? new Link() : null;
            }

            static boolean reserveSpaceForLink(AtomicInteger availableSharedCapacity) {
                for (;;) {
                    int available = availableSharedCapacity.get();
                    if (available < LINK_CAPACITY) {
                        return false;
                    }
                    if (availableSharedCapacity.compareAndSet(available, available - LINK_CAPACITY)) {
                        return true;
                    }
                }
            }
        }

        private final Head head;
        private Link tail;
        private WeakOrderQueue next;
        private final int id = ID_GENERATOR.getAndIncrement();
        private final int interval;
        private int handleRecycleCount;

        private WeakOrderQueue() {
            super(null);
            head = new Head(null);
            interval = 0;
        }

        private WeakOrderQueue(Stack<?> stack, Thread thread) {
            super(thread);
            tail = new Link();
            head = new Head(stack.availableSharedCapacity);
            head.link = tail;
            interval = stack.delayedQueueInterval;
            handleRecycleCount = interval;
        }

        static WeakOrderQueue newQueue(Stack<?> stack, Thread thread) {
            if (!Head.reserveSpaceForLink(stack.availableSharedCapacity)) {
                return null;
            }
            final WeakOrderQueue queue = new WeakOrderQueue(stack, thread);
            stack.setHead(queue);
            return queue;
        }

        WeakOrderQueue getNext() {
            return next;
        }

        void setNext(WeakOrderQueue next) {
            assert next != this;
            this.next = next;
        }

        void reclaimAllSpaceAndUnlink() {
            head.reclaimAllSpaceAndUnlink();
            next = null;
        }

        void add(DefaultHandle<?> handle) {
            if (!handle.compareAndSetLastRecycledId(0, id)) {
                return;
            }

            if (!handle.hasBeenRecycled) {
                if (handleRecycleCount < interval) {
                    handleRecycleCount++;
                    return;
                }
                handleRecycleCount = 0;
            }

            Link tail = this.tail;
            int writeIndex;
            if ((writeIndex = tail.get()) == LINK_CAPACITY) {
                Link link = head.newLink();
                if (link == null) {
                    return;
                }
                this.tail = tail = tail.next = link;
                writeIndex = tail.get();
            }
            tail.elements[writeIndex] = handle;
            handle.stack = null;
            tail.lazySet(writeIndex + 1);
        }

        boolean hasFinalData() {
            return tail.readIndex != tail.get();
        }

        @SuppressWarnings("rawtypes")
        boolean transfer(Stack<?> dst) {
            Link head = this.head.link;
            if (head == null) {
                return false;
            }

            if (head.readIndex == LINK_CAPACITY) {
                if (head.next == null) {
                    return false;
                }
                head = head.next;
                this.head.relink(head);
            }

            final int srcStart = head.readIndex;
            int srcEnd = head.get();
            final int srcSize = srcEnd - srcStart;
            if (srcSize == 0) {
                return false;
            }

            final int dstSize = dst.size;
            final int expectedCapacity = dstSize + srcSize;

            if (expectedCapacity > dst.elements.length) {
                final int actualCapacity = dst.increaseCapacity(expectedCapacity);
                srcEnd = Math.min(srcStart + actualCapacity - dstSize, srcEnd);
            }

            if (srcStart != srcEnd) {
                final DefaultHandle[] srcElems = head.elements;
                final DefaultHandle[] dstElems = dst.elements;
                int newDstSize = dstSize;
                for (int i = srcStart; i < srcEnd; i++) {
                    DefaultHandle<?> element = srcElems[i];
                    if (element.recycleId == 0) {
                        element.recycleId = element.lastRecycledId;
                    } else if (element.recycleId != element.lastRecycledId) {
                        throw new IllegalStateException("recycled multiple times");
                    }
                    srcElems[i] = null;

                    if (dst.dropHandle(element)) {
                        continue;
                    }
                    element.stack = dst;
                    dstElems[newDstSize++] = element;
                }

                if (srcEnd == LINK_CAPACITY && head.next != null) {
                    this.head.relink(head.next);
                }

                head.readIndex = srcEnd;
                if (dst.size == newDstSize) {
                    return false;
                }
                dst.size = newDstSize;
                return true;
            } else {
                return false;
            }
        }
    }

    private static final class Stack<T> {

        final Recyclers<T> parent;
        final WeakReference<Thread> threadRef;
        final AtomicInteger availableSharedCapacity;
        private final int maxCapacity;
        private final int interval;
        private final int delayedQueueInterval;
        private final int maxDelayedQueues;
        DefaultHandle<?>[] elements;
        int size;
        private int handleRecycleCount;
        private WeakOrderQueue cursor, prev;
        private volatile WeakOrderQueue head;

        Stack(Recyclers<T> parent, Thread thread, int maxCapacity, int maxSharedCapacityFactor,
              int interval, int maxDelayedQueues, int delayedQueueInterval) {
            this.parent = parent;
            threadRef = new WeakReference<>(thread);
            this.maxCapacity = maxCapacity;
            availableSharedCapacity = new AtomicInteger(Math.max(maxCapacity / maxSharedCapacityFactor, LINK_CAPACITY));
            elements = new DefaultHandle[Math.min(INITIAL_CAPACITY, maxCapacity)];
            this.interval = interval;
            this.delayedQueueInterval = delayedQueueInterval;
            handleRecycleCount = interval;
            this.maxDelayedQueues = maxDelayedQueues;
        }

        synchronized void setHead(WeakOrderQueue queue) {
            queue.setNext(head);
            head = queue;
        }

        int increaseCapacity(int expectedCapacity) {
            int newCapacity = elements.length;
            int maxCapacity = this.maxCapacity;
            do {
                newCapacity <<= 1;
            } while (newCapacity < expectedCapacity && newCapacity < maxCapacity);

            newCapacity = Math.min(newCapacity, maxCapacity);
            if (newCapacity != elements.length) {
                elements = Arrays.copyOf(elements, newCapacity);
            }

            return newCapacity;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        DefaultHandle<T> pop() {
            int size = this.size;
            if (size == 0) {
                if (!scavenge()) {
                    return null;
                }
                size = this.size;
                if (size <= 0) {
                    return null;
                }
            }
            size--;
            DefaultHandle ret = elements[size];
            elements[size] = null;
            this.size = size;

            if (ret.lastRecycledId != ret.recycleId) {
                throw new IllegalStateException("recycled multiple times");
            }
            ret.recycleId = 0;
            ret.lastRecycledId = 0;
            return ret;
        }

        private boolean scavenge() {
            if (scavengeSome()) {
                return true;
            }
            prev = null;
            cursor = head;
            return false;
        }

        private boolean scavengeSome() {
            WeakOrderQueue prev;
            WeakOrderQueue cursor = this.cursor;
            if (cursor == null) {
                prev = null;
                cursor = head;
                if (cursor == null) {
                    return false;
                }
            } else {
                prev = this.prev;
            }

            boolean success = false;
            do {
                if (cursor.transfer(this)) {
                    success = true;
                    break;
                }
                WeakOrderQueue next = cursor.getNext();
                if (cursor.get() == null) {
                    if (cursor.hasFinalData()) {
                        for (;;) {
                            if (cursor.transfer(this)) {
                                success = true;
                            } else {
                                break;
                            }
                        }
                    }

                    if (prev != null) {
                        cursor.reclaimAllSpaceAndUnlink();
                        prev.setNext(next);
                    }
                } else {
                    prev = cursor;
                }

                cursor = next;

            } while (cursor != null && !success);

            this.prev = prev;
            this.cursor = cursor;
            return success;
        }

        void push(DefaultHandle<?> item) {
            Thread currentThread = Thread.currentThread();
            if (threadRef.get() == currentThread) {
                pushNow(item);
            } else {
                pushLater(item, currentThread);
            }
        }

        private void pushNow(DefaultHandle<?> item) {
            if ((item.recycleId | item.lastRecycledId) != 0
                    || !item.compareAndSetLastRecycledId(0, OWN_THREAD_ID)) {
                throw new IllegalStateException("recycled already");
            }
            item.recycleId = OWN_THREAD_ID;

            int size = this.size;
            if (size >= maxCapacity || dropHandle(item)) {
                return;
            }
            if (size == elements.length) {
                elements = Arrays.copyOf(elements, Math.min(size << 1, maxCapacity));
            }

            elements[size] = item;
            this.size = size + 1;
        }

        private void pushLater(DefaultHandle<?> item, Thread thread) {
            if (maxDelayedQueues == 0) {
                return;
            }

            Map<Stack<?>, WeakOrderQueue> delayedRecycled = DELAYED_RECYCLED.get();
            WeakOrderQueue queue = delayedRecycled.get(this);
            if (queue == null) {
                if (delayedRecycled.size() >= maxDelayedQueues) {
                    delayedRecycled.put(this, WeakOrderQueue.DUMMY);
                    return;
                }
                if ((queue = WeakOrderQueue.newQueue(this, thread)) == null) {
                    return;
                }
                delayedRecycled.put(this, queue);
            } else if (queue == WeakOrderQueue.DUMMY) {
                return;
            }

            queue.add(item);
        }

        boolean dropHandle(DefaultHandle<?> handle) {
            if (!handle.hasBeenRecycled) {
                if (handleRecycleCount < interval) {
                    handleRecycleCount++;
                    return true;
                }
                handleRecycleCount = 0;
                handle.hasBeenRecycled = true;
            }
            return false;
        }

        DefaultHandle<T> newHandle() {
            return new DefaultHandle<>(this);
        }
    }
}
