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

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.rpc.RpcConfigManager;
import com.alipay.remoting.rpc.RpcConfigs;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.codahale.metrics.MetricRegistry;

/**
 * Helper methods for jraft.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-07 10:12:35 AM
 */
public class Utils {

    private static final Logger       LOG                                 = LoggerFactory.getLogger(Utils.class);

    /**
     * Current system CPUs count.
     */
    private static final int          CPUS                                = Runtime.getRuntime().availableProcessors();

    /**
     * Default jraft closure executor pool minimum size, CPUs by default.
     */
    public static final int           MIN_CLOSURE_EXECUTOR_POOL_SIZE      = SystemPropertyUtil.getInt(
                                                                              "jraft.closure.threadpool.size.min",
                                                                              cpus());

    /**
     * Default jraft closure executor pool maximum size.
     */
    public static final int           MAX_CLOSURE_EXECUTOR_POOL_SIZE      = SystemPropertyUtil.getInt(
                                                                              "jraft.closure.threadpool.size.max",
                                                                              Math.max(100, cpus() * 5));

    /**
     * Default jraft append-entries executor(send) pool size.
     */
    public static final int           APPEND_ENTRIES_THREADS_SEND         = SystemPropertyUtil
                                                                              .getInt(
                                                                                  "jraft.append.entries.threads.send",
                                                                                  Math.max(
                                                                                      16,
                                                                                      Ints.findNextPositivePowerOfTwo(cpus() * 2)));

    /**
     * Default jraft append-entries executor(receive) pool size.
     */
    public static final int           APPEND_ENTRIES_THREADS_RECV         = SystemPropertyUtil
                                                                              .getInt(
                                                                                  "jraft.append.entries.threads.recv",
                                                                                  Math.max(
                                                                                      16,
                                                                                      Ints.findNextPositivePowerOfTwo(cpus() * 2)));

    /**
     * Default jraft max pending tasks of append-entries per thread, 65536 by default.
     */
    public static final int           MAX_APPEND_ENTRIES_TASKS_PER_THREAD = SystemPropertyUtil
                                                                              .getInt(
                                                                                  "jraft.max.append.entries.tasks.per.thread",
                                                                                  32768);

    /**
     * Whether use {@link com.alipay.sofa.jraft.util.concurrent.MpscSingleThreadExecutor}, true by default.
     */
    public static final boolean       USE_MPSC_SINGLE_THREAD_EXECUTOR     = SystemPropertyUtil.getBoolean(
                                                                              "jraft.use.mpsc.single.thread.executor",
                                                                              true);

    /**
     * Global thread pool to run closure.
     */
    private static ThreadPoolExecutor CLOSURE_EXECUTOR                    = ThreadPoolUtil
                                                                              .newBuilder()
                                                                              .poolName("JRAFT_CLOSURE_EXECUTOR")
                                                                              .enableMetric(true)
                                                                              .coreThreads(
                                                                                  MIN_CLOSURE_EXECUTOR_POOL_SIZE)
                                                                              .maximumThreads(
                                                                                  MAX_CLOSURE_EXECUTOR_POOL_SIZE)
                                                                              .keepAliveSeconds(60L)
                                                                              .workQueue(new SynchronousQueue<>())
                                                                              .threadFactory(
                                                                                  new NamedThreadFactory(
                                                                                      "JRaft-Closure-Executor-", true))
                                                                              .build();

    private static final Pattern      GROUP_ID_PATTER                     = Pattern
                                                                              .compile("^[a-zA-Z][a-zA-Z0-9\\-_]*$");

    public static void verifyGroupId(final String groupId) {
        if (StringUtils.isBlank(groupId)) {
            throw new IllegalArgumentException("Blank groupId");
        }
        if (!GROUP_ID_PATTER.matcher(groupId).matches()) {
            throw new IllegalArgumentException(
                "Invalid group id, it should be started with character 'a'-'z' or 'A'-'Z',"
                        + "and followed with numbers, english alphabet, '-' or '_'. ");
        }
    }

    /**
     * Register CLOSURE_EXECUTOR into metric registry.
     */
    public static void registerClosureExecutorMetrics(final MetricRegistry registry) {
        registry.register("raft-utils-closure-thread-pool", new ThreadPoolMetricSet(CLOSURE_EXECUTOR));
    }

    /**
     * Run closure with OK status in thread pool.
     */
    public static Future<?> runClosureInThread(final Closure done) {
        if (done == null) {
            return null;
        }
        return runClosureInThread(done, Status.OK());
    }

    /**
     * Run a task in thread pool,returns the future object.
     */
    public static Future<?> runInThread(final Runnable runnable) {
        return CLOSURE_EXECUTOR.submit(runnable);
    }

    /**
     * Run closure with status in thread pool.
     */
    public static Future<?> runClosureInThread(final Closure done, final Status status) {
        if (done == null) {
            return null;
        }
        return runInThread(() -> {
            try {
                done.run(status);
            } catch (final Throwable t) {
                LOG.error("Fail to run done closure", t);
            }
        });
    }

    /**
     * Close a closeable.
     */
    public static int closeQuietly(final Closeable closeable) {
        if (closeable == null) {
            return 0;
        }
        try {
            closeable.close();
            return 0;
        } catch (final IOException e) {
            LOG.error("Fail to close", e);
            return RaftError.EIO.getNumber();
        }
    }

    /**
     * Get system CPUs count.
     */
    public static int cpus() {
        return CPUS;
    }

    /**
     * Get java process id.
     */
    public static long getProcessId(final long fallback) {
        // Note: may fail in some JVM implementations
        // therefore fallback has to be provided

        // something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
        final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        final int index = jvmName.indexOf('@');

        if (index < 1) {
            // part before '@' empty (index = 0) / '@' not found (index = -1)
            return fallback;
        }

        try {
            return Long.parseLong(jvmName.substring(0, index));
        } catch (final NumberFormatException e) {
            // ignore
        }
        return fallback;
    }

    /**
     * Default init and expand buffer size, it can be set by -Djraft.byte_buf.size=n, default 1024.
     */
    public static final int RAFT_DATA_BUF_SIZE            = Integer.parseInt(System.getProperty("jraft.byte_buf.size",
                                                              "1024"));

    /**
     * Default max {@link ByteBufferCollector} size per thread for recycle, it can be set by
     * -Djraft.max_collector_size_per_thread, default 256
     */
    public static final int MAX_COLLECTOR_SIZE_PER_THREAD = Integer.parseInt(System.getProperty(
                                                              "jraft.max_collector_size_per_thread", "256"));


    /**
     * Expand byte buffer for 1024 bytes.
     */
    public static ByteBuffer expandByteBuffer(final ByteBuffer buf) {
        return expandByteBufferAtLeast(buf, RAFT_DATA_BUF_SIZE);
    }

    /**
     * Allocate a byte buffer with size.
     */
    public static ByteBuffer allocate(final int size) {
        return ByteBuffer.allocate(size);
    }

    /**
     * Allocate a byte buffer with {@link #RAFT_DATA_BUF_SIZE}
     */
    public static ByteBuffer allocate() {
        return allocate(RAFT_DATA_BUF_SIZE);
    }

    /**
     * Expand byte buffer at least minLength.
     */
    public static ByteBuffer expandByteBufferAtLeast(final ByteBuffer buf, final int minLength) {
        final int newCapacity = minLength > RAFT_DATA_BUF_SIZE ? minLength : RAFT_DATA_BUF_SIZE;
        final ByteBuffer newBuf = ByteBuffer.allocate(buf.capacity() + newCapacity);
        buf.flip();
        newBuf.put(buf);
        return newBuf;
    }

    /**
     * Expand byte buffer at most maxLength.
     */
    public static ByteBuffer expandByteBufferAtMost(final ByteBuffer buf, final int maxLength) {
        final int newCapacity = maxLength > RAFT_DATA_BUF_SIZE || maxLength <= 0 ? RAFT_DATA_BUF_SIZE : maxLength;
        final ByteBuffer newBuf = ByteBuffer.allocate(buf.capacity() + newCapacity);
        buf.flip();
        newBuf.put(buf);
        return newBuf;
    }

    /**
     * ANY IP address 0.0.0.0
     */
    public static final String IP_ANY = "0.0.0.0";

    /**
     * Gets the current monotonic time in milliseconds.
     */
    public static long monotonicMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    /**
     * Returns the current time in milliseconds, it's not monotonic,
     * would be forwarded/backward by clock synchronous.
     */
    public static long nowMs() {
        return System.currentTimeMillis();
    }

    /**
     * Gets the current monotonic time in microseconds.
     */
    public static long monotonicUs() {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
    }

    /**
     * Get string bytes in UTF-8 charset.
     */
    public static byte[] getBytes(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Ensure bolt RPC framework supports pipeline, enable `bolt.rpc.dispatch-msg-list-in-default-executor`
     * system property.
     */
    public static void ensureBoltPipeline() {
        if (RpcConfigManager.dispatch_msg_list_in_default_executor()) {
            System.setProperty(RpcConfigs.DISPATCH_MSG_LIST_IN_DEFAULT_EXECUTOR, "false");
            RaftRpcServerFactory.LOG.warn("JRaft SET {} to be false for replicator pipeline optimistic.",
                RpcConfigs.DISPATCH_MSG_LIST_IN_DEFAULT_EXECUTOR);
        }
    }

    public static <T> T withLockObject(final T obj) {
        return Requires.requireNonNull(obj, "obj");
    }
}
