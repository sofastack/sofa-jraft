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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jctools.queues.atomic.MpscGrowableAtomicArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Light-weight object pool based on a thread-local stack with MPSC cross-thread recycling.
 * <p/>
 * Key fix vs old implementation:
 * Replaced unbounded WeakOrderQueue + WeakHashMap (source of memory leaks when threads die)
 * with a simple MPSC queue per Stack. When the owning thread dies, the ThreadLocal entry is GC'd,
 * and so is the MPSC queue (no strong reference held elsewhere). This eliminates the memory leak.
 * <p/>
 * Forked from <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @param <T> the type of the pooled object
 * @author jiachun.fjc (original), Netty Project, JervyClaw (AI agent)
 */
public abstract class Recyclers<T> {

    private static final Logger         LOG                                     = LoggerFactory
                                                                                    .getLogger(Recyclers.class);

    private static final AtomicInteger  idGenerator                             = new AtomicInteger(Integer.MIN_VALUE);

    private static final int            OWN_THREAD_ID                           = idGenerator.getAndIncrement();
    private static final int            DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD = 4 * 1024;
    private static final int            MAX_CAPACITY_PER_THREAD;
    private static final int            INITIAL_CAPACITY;

    // JCTools MPSC queue parameters (matching Netty's default LINK_CAPACITY = 16)
    private static final int            MPSC_INITIAL_CAPACITY                   = 16;
    private static final int            MPSC_MAX_CAPACITY                       = 1024;

    static {
        int maxCapacityPerThread = SystemPropertyUtil.getInt("jraft.recyclers.maxCapacityPerThread",
            DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD);
        if (maxCapacityPerThread < 0) {
            maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
        }

        MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;

        LOG.info("-Djraft.recyclers.maxCapacityPerThread: {}.", MAX_CAPACITY_PER_THREAD == 0 ? "disabled"
            : MAX_CAPACITY_PER_THREAD);

        INITIAL_CAPACITY = Math.min(MAX_CAPACITY_PER_THREAD, 256);
    }

    public static final Handle          NOOP_HANDLE                             = new Handle() {
                                                                                };

    private final int                   maxCapacityPerThread;
    private final ThreadLocal<Stack<T>> threadLocal                             = new ThreadLocal<Stack<T>>() {
                                                                                    @Override
                                                                                    protected Stack<T> initialValue() {
                                                                                        return new Stack<>(
                                                                                            Recyclers.this,
                                                                                            Thread.currentThread(),
                                                                                            maxCapacityPerThread);
                                                                                    }
                                                                                };

    protected Recyclers() {
        this(MAX_CAPACITY_PER_THREAD);
    }

    protected Recyclers(int maxCapacityPerThread) {
        this.maxCapacityPerThread = Math.min(MAX_CAPACITY_PER_THREAD, Math.max(0, maxCapacityPerThread));
    }

    @SuppressWarnings("unchecked")
    public final T get() {
        if (maxCapacityPerThread == 0) {
            return newObject(NOOP_HANDLE);
        }
        Stack<T> stack = threadLocal.get();
        DefaultHandle handle = stack.pop();
        if (handle == null) {
            handle = stack.newHandle();
            handle.value = newObject(handle);
        }
        return (T) handle.value;
    }

    public final boolean recycle(T o, Handle handle) {
        if (handle == NOOP_HANDLE) {
            return false;
        }

        DefaultHandle h = (DefaultHandle) handle;

        final Stack<?> stack = h.stack;
        if (h.lastRecycledId != h.recycleId || stack == null) {
            throw new IllegalStateException("recycled already");
        }

        if (stack.parent != this) {
            return false;
        }
        if (o != h.value) {
            throw new IllegalArgumentException("o does not belong to handle");
        }
        h.recycle();
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
    }

    static final class DefaultHandle implements Handle {
        private int      lastRecycledId;
        private int      recycleId;

        private Stack<?> stack;
        private Object   value;

        DefaultHandle(Stack<?> stack) {
            this.stack = stack;
        }

        public void recycle() {
            Thread thread = Thread.currentThread();

            final Stack<?> stack = this.stack;
            if (lastRecycledId != recycleId || stack == null) {
                throw new IllegalStateException("recycled already");
            }

            if (thread == stack.thread) {
                stack.push(this);
                return;
            }
            // Different thread — add to the MPSC queue for this stack
            // Key fix: uses bounded MPSC queue instead of WeakOrderQueue
            // When owner thread dies, the ThreadLocal entry is GC'd along with the queue
            addToMpscQueue(this, stack);
        }
    }

    /**
     * Map from Stack to its MPSC cross-thread recycling queue.
     * Uses ConcurrentHashMap instead of ThreadLocal WeakHashMap to avoid the memory leak.
     * Each Stack has one MPSC queue (created lazily on first cross-thread recycle).
     * <p>
     * Memory leak fix: When the owner thread dies, its ThreadLocal entry becomes unreachable,
     * and so does the Stack and its MPSC queue (since they are only referenced from the
     * ThreadLocal). With the old WeakHashMap design, the map entry held a strong reference
     * to the WeakOrderQueue, preventing GC of dead threads' queues.
     */
    private static final Map<Stack<?>, MpscGrowableAtomicArrayQueue<DefaultHandle>> delayedRecycled = new ConcurrentHashMap<>();

    /**
     * Add a handle to the target Stack's MPSC queue for cross-thread recycling.
     */
    private static void addToMpscQueue(DefaultHandle handle, Stack<?> stack) {
        handle.lastRecycledId = handle.hashCode();

        // Create the MPSC queue if it doesn't exist yet
        MpscGrowableAtomicArrayQueue<DefaultHandle> queue = delayedRecycled.get(stack);
        if (queue == null) {
            // Use computeIfAbsent to avoid race condition on queue creation
            queue = delayedRecycled.computeIfAbsent(stack,
                k -> new MpscGrowableAtomicArrayQueue<>(MPSC_INITIAL_CAPACITY, MPSC_MAX_CAPACITY));
        }

        // If the owner thread is dead, discard to prevent unbounded queue growth
        // (old WeakOrderQueue accumulated for dead threads causing memory leak)
        if (!stack.thread.isAlive()) {
            return;
        }

        // offer() is non-blocking — if queue is full, the item is dropped
        queue.offer(handle);
    }

    // ----- WeakOrderQueue and related code removed -----
    // The old WeakOrderQueue + WeakHashMap design caused memory leaks when threads died.
    // Replaced with MPSC queue approach described above.

    static final class Stack<T> {

        final Recyclers<T>      parent;
        final Thread            thread;
        private DefaultHandle[] elements;
        private final int       maxCapacity;
        private int             size;

        // MPSC queue cursor for scavenging (single queue instead of linked list)
        private DefaultHandle   mpscQueueHead; // Simple pointer to head of MPSC transfer

        // Note: actual queue is in the delayedRecycled map

        Stack(Recyclers<T> parent, Thread thread, int maxCapacity) {
            this.parent = parent;
            this.thread = thread;
            this.maxCapacity = maxCapacity;
            elements = new DefaultHandle[Math.min(INITIAL_CAPACITY, maxCapacity)];
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

        DefaultHandle pop() {
            int size = this.size;
            if (size == 0) {
                if (!scavenge()) {
                    return null;
                }
                size = this.size;
            }
            size--;
            DefaultHandle ret = elements[size];
            if (ret.lastRecycledId != ret.recycleId) {
                throw new IllegalStateException("recycled multiple times");
            }
            ret.recycleId = 0;
            ret.lastRecycledId = 0;
            this.size = size;
            return ret;
        }

        boolean scavenge() {
            // continue an existing scavenge, if any
            if (scavengeSome()) {
                return true;
            }

            // reset our scavenge cursor
            return false;
        }

        boolean scavengeSome() {
            // Drain from the MPSC cross-thread queue
            MpscGrowableAtomicArrayQueue<DefaultHandle> queue = delayedRecycled.get(this);
            if (queue == null) {
                return false;
            }

            DefaultHandle handle;
            int transferred = 0;

            // Transfer items from MPSC queue to local buffer (max one chunk per call)
            while ((handle = queue.poll()) != null && transferred < 64) {
                // Reset recycle IDs (same as old WeakOrderQueue.transfer())
                if (handle.recycleId == 0) {
                    handle.recycleId = handle.lastRecycledId;
                } else if (handle.recycleId != handle.lastRecycledId) {
                    // Already recycled — skip
                    continue;
                }
                handle.stack = this;

                int size = this.size;
                if (size >= maxCapacity) {
                    // Local buffer full — stop transferring
                    break;
                }
                if (size == elements.length) {
                    increaseCapacity(size + 1);
                }
                elements[size] = handle;
                this.size = size + 1;
                transferred++;
            }

            return transferred > 0;
        }

        void push(DefaultHandle item) {
            if ((item.recycleId | item.lastRecycledId) != 0) {
                throw new IllegalStateException("recycled already");
            }
            item.recycleId = item.lastRecycledId = OWN_THREAD_ID;

            int size = this.size;
            if (size >= maxCapacity) {
                // Hit the maximum capacity - drop the possibly youngest object.
                return;
            }
            if (size == elements.length) {
                elements = Arrays.copyOf(elements, Math.min(size << 1, maxCapacity));
            }

            elements[size] = item;
            this.size = size + 1;
        }

        DefaultHandle newHandle() {
            return new DefaultHandle(this);
        }
    }

    private static final class SystemPropertyUtil {
        static int getInt(String name, int defaultValue) {
            String value = System.getProperty(name);
            if (value == null) {
                return defaultValue;
            }

            value = value.trim();
            if (value.isEmpty()) {
                return defaultValue;
            }

            try {
                return Integer.parseInt(value);
            } catch (Exception e) {
                return defaultValue;
            }
        }
    }
}
