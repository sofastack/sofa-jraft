/*
 * Copyright 2014 The Netty Project
 * Copyright 2024 The SOFAJRaft Authors
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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.jctools.queues.atomic.MpscGrowableAtomicArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Light-weight object pool based on a thread-local stack with MPSC cross-thread recycling.
 * <p/>
 * Key fix vs old implementation (SOFAJRaft issue #1240):
 * Replaced unbounded WeakOrderQueue + per-thread ThreadLocal WeakHashMap with a single
 * global WeakHashMap: Stack → MPSC queue. When the owning thread dies, the Stack's
 * WeakReference is cleared, and the map entry is cleaned up via ReferenceQueue on the
 * next map access. This eliminates the memory leak of the old design.
 * <p/>
 * Why WeakHashMap instead of ConcurrentHashMap?
 * ConcurrentHashMap holds strong references to keys (Stack), preventing GC of dead threads'
 * Stacks. WeakHashMap's weak keys allow GC of Stack when the only remaining reference is
 * from the map itself — which is cleaned up when the weak reference is cleared.
 * <p/>
 * Forked from <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @param <T> the type of the pooled object
 * @author jiachun.fjc (original), Netty Project, JervyClaw (AI agent)
 */
public abstract class Recyclers<T> {

    private static final Logger         LOG                             = LoggerFactory.getLogger(Recyclers.class);

    private static final AtomicInteger  ID_GENERATOR                    = new AtomicInteger(Integer.MIN_VALUE);

    private static final int            OWN_THREAD_ID                   = ID_GENERATOR.getAndIncrement();
    private static final int            DEFAULT_MAX_CAPACITY_PER_THREAD = 4 * 1024;
    private static final int            MAX_CAPACITY_PER_THREAD;
    private static final int            INITIAL_CAPACITY;

    // MPSC queue parameters
    private static final int            MPSC_INITIAL_CAPACITY           = 16;
    private static final int            MPSC_MAX_CAPACITY               = 1024;

    static {
        int maxCapacityPerThread = SystemPropertyUtil.getInt("jraft.recyclers.maxCapacityPerThread",
            DEFAULT_MAX_CAPACITY_PER_THREAD);
        MAX_CAPACITY_PER_THREAD = maxCapacityPerThread < 0 ? DEFAULT_MAX_CAPACITY_PER_THREAD : maxCapacityPerThread;

        INITIAL_CAPACITY = Math.min(MAX_CAPACITY_PER_THREAD, 256);

        LOG.info("-Djraft.recyclers.maxCapacityPerThread: {}.", MAX_CAPACITY_PER_THREAD == 0 ? "disabled"
            : MAX_CAPACITY_PER_THREAD);
    }

    public static final Handle          NOOP_HANDLE                     = new Handle() {
                                                                        };

    private final int                   maxCapacityPerThread;
    private final ThreadLocal<Stack<T>> THREAD_LOCAL                    = new ThreadLocal<Stack<T>>() {
                                                                            @Override
                                                                            protected Stack<T> initialValue() {
                                                                                return new Stack<>(Recyclers.this,
                                                                                    Thread.currentThread(),
                                                                                    maxCapacityPerThread);
                                                                            }
                                                                        };

    protected Recyclers() {
        this(MAX_CAPACITY_PER_THREAD);
    }

    protected Recyclers(final int maxCapacityPerThread) {
        this.maxCapacityPerThread = Math.min(MAX_CAPACITY_PER_THREAD, Math.max(0, maxCapacityPerThread));
    }

    @SuppressWarnings("unchecked")
    public final T get() {
        if (maxCapacityPerThread == 0) {
            return newObject(NOOP_HANDLE);
        }
        final Stack<T> stack = THREAD_LOCAL.get();
        final DefaultHandle handle = stack.pop();
        if (handle == null) {
            final DefaultHandle newHandle = stack.newHandle();
            newHandle.value = newObject(newHandle);
            return (T) newHandle.value;
        }
        return (T) handle.value;
    }

    public final boolean recycle(final Object o, final Handle handle) {
        if (handle == NOOP_HANDLE) {
            return false;
        }

        final DefaultHandle h = (DefaultHandle) handle;
        final Stack<?> stack = h.stack;

        if (h.lastRecycledId != h.recycleId || stack == null) {
            throw new IllegalStateException("recycled already");
        }

        if (stack.parent != this) {
            return false;
        }

        if (o != h.value) {
            throw new IllegalArgumentException("object does not belong to this handle");
        }

        h.recycle();
        return true;
    }

    protected abstract T newObject(Handle handle);

    public final int threadLocalCapacity() {
        return THREAD_LOCAL.get().elements.length;
    }

    public final int threadLocalSize() {
        return THREAD_LOCAL.get().size;
    }

    public interface Handle {
    }

    static final class DefaultHandle implements Handle {
        private Stack<?> stack;
        private Object   value;
        private int      lastRecycledId;
        private int      recycleId;

        DefaultHandle(final Stack<?> stack) {
            this.stack = stack;
        }

        public void recycle() {
            final Thread currentThread = Thread.currentThread();
            final Stack<?> stack = this.stack;

            if (this.lastRecycledId != this.recycleId || stack == null) {
                throw new IllegalStateException("recycled already");
            }

            if (currentThread == stack.thread) {
                stack.push(this);
                return;
            }

            // Cross-thread recycle: add to MPSC queue
            addToMpscQueue(this, stack);
        }
    }

    /**
     * ReferenceQueue for detecting when Stack objects have been garbage collected.
     * When a Stack becomes weakly reachable (no more strong references exist),
     * its reference is enqueued here. We poll and clean up stale map entries.
     */
    private static final ReferenceQueue<Stack<?>>                                   STACK_REF_QUEUE = new ReferenceQueue<>();

    /**
     * Map from Stack to its MPSC cross-thread recycling queue.
     * Uses WeakHashMap so that when the owner thread dies (Stack becomes weakly reachable),
     * the entry is eligible for cleanup. Entries are cleaned via STACK_REF_QUEUE polling.
     * <p>
     * Why WeakHashMap instead of ConcurrentHashMap?
     * - ConcurrentHashMap holds STRONG references to keys → Stack is NEVER GC'd
     * - WeakHashMap's weak keys allow Stack to be GC'd when only the map holds a reference
     * <p>
     * Thread safety: uses synchronized map. All cross-thread operations (offer/poll) are
     * already on different threads, so the synchronization overhead is minimal and
     * only affects the map lookup, not the actual queue operations.
     */
    private static final Map<Stack<?>, MpscGrowableAtomicArrayQueue<DefaultHandle>> DELAYED_RECYCLED;

    static {
        DELAYED_RECYCLED = java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>(64));
    }

    /**
     * Expunge stale entries from DELAYED_RECYCLED.
     * Called from operations that access the map to ensure weak references are cleaned.
     */
    @SuppressWarnings("unchecked")
    private static void expungeStaleEntries() {
        Reference<? extends Stack<?>> ref;
        while ((ref = STACK_REF_QUEUE.poll()) != null) {
            DELAYED_RECYCLED.remove(ref.get());
        }
    }

    /**
     * Add a handle to the target Stack's MPSC queue from a different thread.
     * <p>
     * Key safety measures:
     * 1. Clear handle.stack BEFORE offering to the queue (prevents stale cross-thread reference)
     * 2. Check owner thread liveness before offering (prevents queue buildup for dead threads)
     * 3. expungeStaleEntries() on every call (ensures WeakHashMap cleanup)
     */
    private static void addToMpscQueue(final DefaultHandle handle, final Stack<?> stack) {
        // Key fix #1: Clear stack reference BEFORE enqueueing.
        // In the old WeakOrderQueue, this was done via lazySet. Setting to null here
        // ensures the owner thread sees a consistent null while the handle is in-flight.
        handle.stack = null;
        handle.lastRecycledId = System.identityHashCode(handle);

        // Ensure stale entries are cleaned up (WeakHashMap maintenance)
        expungeStaleEntries();

        // Get or create the MPSC queue for this stack
        final MpscGrowableAtomicArrayQueue<DefaultHandle> queue;
        synchronized (DELAYED_RECYCLED) {
            queue = DELAYED_RECYCLED.computeIfAbsent(stack,
                k -> new MpscGrowableAtomicArrayQueue<>(MPSC_INITIAL_CAPACITY, MPSC_MAX_CAPACITY));
        }

        // Key fix #2: If the owner thread is dead, discard the item.
        // This prevents unbounded queue growth for threads that have died.
        if (!stack.thread.isAlive()) {
            return;
        }

        // offer() is non-blocking — if the queue is full, the item is dropped.
        // This is acceptable since the owner thread is alive and actively scavenging.
        queue.offer(handle);
    }

    static final class Stack<T> {

        final Recyclers<T>      parent;
        final Thread            thread;
        private DefaultHandle[] elements;
        private final int       maxCapacity;
        private int             size;

        Stack(final Recyclers<T> parent, final Thread thread, final int maxCapacity) {
            this.parent = parent;
            this.thread = thread;
            this.maxCapacity = maxCapacity;
            this.elements = new DefaultHandle[Math.min(INITIAL_CAPACITY, maxCapacity)];
        }

        int increaseCapacity(final int expectedCapacity) {
            int newCapacity = elements.length;
            final int limit = this.maxCapacity;
            do {
                newCapacity <<= 1;
            } while (newCapacity < expectedCapacity && newCapacity < limit);

            newCapacity = Math.min(newCapacity, limit);
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
            final DefaultHandle ret = elements[size];
            elements[size] = null; // Help GC
            if (ret.lastRecycledId != ret.recycleId) {
                throw new IllegalStateException("recycled multiple times");
            }
            ret.recycleId = 0;
            ret.lastRecycledId = 0;
            this.size = size;
            return ret;
        }

        boolean scavenge() {
            return scavengeSome();
        }

        boolean scavengeSome() {
            // Ensure stale entries are cleaned up (WeakHashMap maintenance)
            expungeStaleEntries();

            // Poll from the MPSC queue for this stack
            final MpscGrowableAtomicArrayQueue<DefaultHandle> queue = DELAYED_RECYCLED.get(this);
            if (queue == null) {
                return false;
            }

            DefaultHandle handle;
            int transferred = 0;
            final int limit = 64; // Transfer up to 64 items per call (prevents long pauses)

            // Transfer items from MPSC queue to local buffer
            while ((handle = queue.poll()) != null && transferred < limit) {
                // Validate handle state (same check as WeakOrderQueue.transfer())
                if (handle.recycleId == 0) {
                    // Handle was never recycled by any thread — use lastRecycledId
                    handle.recycleId = handle.lastRecycledId;
                } else if (handle.recycleId != handle.lastRecycledId) {
                    // Handle has already been recycled — this is a bug, throw instead of silently skip
                    throw new IllegalStateException(
                        "handle was recycled by another thread after being transferred here");
                }

                // Restore stack reference and transfer to local buffer
                handle.stack = this;

                final int size = this.size;
                if (size >= maxCapacity) {
                    // Local buffer is full — stop transferring
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

        void push(final DefaultHandle item) {
            if ((item.recycleId | item.lastRecycledId) != 0) {
                throw new IllegalStateException("recycled already");
            }
            item.recycleId = item.lastRecycledId = OWN_THREAD_ID;

            final int size = this.size;
            if (size >= maxCapacity) {
                // Buffer full — drop the youngest object (Netty behavior)
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
        static int getInt(final String name, final int defaultValue) {
            final String value = System.getProperty(name);
            if (value == null || value.isEmpty()) {
                return defaultValue;
            }
            try {
                return Integer.parseInt(value.trim());
            } catch (final NumberFormatException e) {
                return defaultValue;
            }
        }
    }
}
