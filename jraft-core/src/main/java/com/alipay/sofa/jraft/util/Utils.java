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
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.util.concurrent.DefaultFixedThreadsExecutorGroupFactory;
import com.alipay.sofa.jraft.util.concurrent.FixedThreadsExecutorGroup;
import com.codahale.metrics.MetricRegistry;

/**
 * Helper methods for jraft.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * <p>
 * 2018-Apr-07 10:12:35 AM
 */
public final class Utils {

    private static final Logger       LOG                                 = LoggerFactory.getLogger(Utils.class);

    /**
     * The configured number of available processors. The default is
     * {@link Runtime#availableProcessors()}. This can be overridden by setting the system property
     * "jraft.available_processors".
     */
    private static final int          CPUS                                = SystemPropertyUtil.getInt(
                                                                              "jraft.available_processors", Runtime
                                                                                  .getRuntime().availableProcessors());

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
     * Default jraft max pending tasks of append-entries per thread, 65536 by default.
     */
    public static final int           MAX_APPEND_ENTRIES_TASKS_PER_THREAD = SystemPropertyUtil
                                                                              .getInt(
                                                                                  "jraft.max.append.entries.tasks.per.thread",
                                                                                  32768);

    /**
     * Whether use {@link com.alipay.sofa.jraft.util.concurrent.MpscSingleThreadExecutor}, true by
     * default.
     */
    public static final boolean       USE_MPSC_SINGLE_THREAD_EXECUTOR     = SystemPropertyUtil.getBoolean(
                                                                              "jraft.use.mpsc.single.thread.executor",
                                                                              true);

    /**
     * Global thread pool to run closure.
     */
    @Deprecated
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

    private static class AppendEntriesThreadPoolHolder {
        private static final FixedThreadsExecutorGroup INSTANCE = DefaultFixedThreadsExecutorGroupFactory.INSTANCE
                                                                    .newExecutorGroup(
                                                                        Utils.APPEND_ENTRIES_THREADS_SEND,
                                                                        "Append-Entries-Thread-Send",
                                                                        Utils.MAX_APPEND_ENTRIES_TASKS_PER_THREAD, true); ;

    }

    public static FixedThreadsExecutorGroup getDefaultAppendEntriesExecutor() {
        return AppendEntriesThreadPoolHolder.INSTANCE;
    }

    public static void verifyGroupId(final String groupId) {
        if (StringUtils.isBlank(groupId)) {
            throw new IllegalArgumentException("Blank groupId");
        }
        if (!GROUP_ID_PATTER.matcher(groupId).matches()) {
            throw new IllegalArgumentException(
                "Invalid group id, it should be started with character 'a'-'z' or 'A'-'Z',"
                        + " and followed with numbers, english alphabet, '-' or '_'. ");
        }
    }

    /**
     * Register CLOSURE_EXECUTOR into metric registry.
     * Deprecated, {@link com.alipay.sofa.jraft.util.ThreadPoolsFactory}
     */
    @Deprecated
    public static void registerClosureExecutorMetrics(final MetricRegistry registry) {
        registry.register("raft-utils-closure-thread-pool", new ThreadPoolMetricSet(CLOSURE_EXECUTOR));
    }

    /**
     * Run closure in current thread.
     *
     * @param done
     * @param status
     */
    public static void runClosure(final Closure done, final Status status) {
        if (done != null) {
            done.run(status);
        }
    }

    /**
     * Run closure with OK status in thread pool.
     * Deprecated, {@link com.alipay.sofa.jraft.util.ThreadPoolsFactory}
     */
    @Deprecated
    public static Future<?> runClosureInThread(final Closure done) {
        if (done == null) {
            return null;
        }
        return runClosureInThread(done, Status.OK());
    }

    /**
     * Run a task in thread pool,returns the future object.
     * Deprecated, {@link com.alipay.sofa.jraft.util.ThreadPoolsFactory}
     */
    @Deprecated
    public static Future<?> runInThread(final Runnable runnable) {
        return CLOSURE_EXECUTOR.submit(runnable);
    }

    /**
     * Run closure with status in thread pool.
     * Deprecated, {@link com.alipay.sofa.jraft.util.ThreadPoolsFactory}
     */
    @SuppressWarnings("Convert2Lambda")
    @Deprecated
    public static Future<?> runClosureInThread(final Closure done, final Status status) {
        if (done == null) {
            return null;
        }

        return runInThread(new Runnable() {

            @Override
            public void run() {
                try {
                    done.run(status);
                } catch (final Throwable t) {
                    LOG.error("Fail to run done closure", t);
                }
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
            LOG.error("Fail to close {}.", closeable, e);
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
    public static final int RAFT_DATA_BUF_SIZE            = SystemPropertyUtil.getInt("jraft.byte_buf.size", 1024);

    /**
     * Default max {@link ByteBufferCollector} size per thread for recycle, it can be set by
     * -Djraft.max_collector_size_per_thread, default 256
     */
    public static final int MAX_COLLECTOR_SIZE_PER_THREAD = SystemPropertyUtil.getInt(
                                                              "jraft.max_collector_size_per_thread", 256);

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
        BufferUtils.flip(buf);
        newBuf.put(buf);
        return newBuf;
    }

    /**
     * Expand byte buffer at most maxLength.
     */
    public static ByteBuffer expandByteBufferAtMost(final ByteBuffer buf, final int maxLength) {
        final int newCapacity = maxLength > RAFT_DATA_BUF_SIZE || maxLength <= 0 ? RAFT_DATA_BUF_SIZE : maxLength;
        final ByteBuffer newBuf = ByteBuffer.allocate(buf.capacity() + newCapacity);
        BufferUtils.flip(buf);
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
     * Returns the current time in milliseconds, it's not monotonic, would be forwarded/backward by
     * clock synchronous.
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

    public static <T> T withLockObject(final T obj) {
        return Requires.requireNonNull(obj, "obj");
    }

    @SuppressWarnings("ConstantConditions")
    public static boolean atomicMoveFile(final File source, final File target, final boolean sync) throws IOException {
        // Move temp file to target path atomically.
        // The code comes from
        // https://github.com/jenkinsci/jenkins/blob/master/core/src/main/java/hudson/util/AtomicFileWriter.java#L187
        Requires.requireNonNull(source, "source");
        Requires.requireNonNull(target, "target");
        final Path sourcePath = source.toPath();
        final Path targetPath = target.toPath();
        boolean success;
        try {
            success = Files.move(sourcePath, targetPath, StandardCopyOption.ATOMIC_MOVE) != null;
        } catch (final IOException e) {
            // If it falls here that can mean many things. Either that the atomic move is not supported,
            // or something wrong happened. Anyway, let's try to be over-diagnosing
            if (e instanceof AtomicMoveNotSupportedException) {
                LOG.warn("Atomic move not supported, falling back to non-atomic move, error: {}.", e.getMessage());
            } else {
                LOG.error("Unable to move atomically, falling back to non-atomic move, error: {}.", e);
            }

            if (target.exists()) {
                LOG.info("The target file {} was already existing.", targetPath);
            }

            try {
                success = Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING) != null;
            } catch (final IOException e1) {
                e1.addSuppressed(e);
                LOG.error("Unable to move {} to {}. Attempting to delete {} and abandoning.", sourcePath, targetPath,
                    sourcePath, e1);
                try {
                    Files.deleteIfExists(sourcePath);
                } catch (final IOException e2) {
                    e2.addSuppressed(e1);
                    LOG.error("Unable to delete {}, good bye then!", sourcePath, e2);
                    throw e2;
                }

                throw e1;
            }
        }
        if (success && sync) {
            File dir = target.getParentFile();
            // fsync on target parent dir.
            fsync(dir);
        }
        return success;
    }

    /**
     * Calls fsync on a file or directory.
     *
     * @param file file or directory
     * @throws IOException if an I/O error occurs
     */
    public static void fsync(final File file) throws IOException {
        final boolean isDir = file.isDirectory();
        // can't fsync on windows.
        if (isDir && Platform.isWindows()) {
            LOG.warn("Unable to fsync directory {} on windows.", file);
            return;
        }
        try (final FileChannel fc = FileChannel.open(file.toPath(), isDir ? StandardOpenOption.READ
            : StandardOpenOption.WRITE)) {
            fc.force(true);
        }
    }

    /**
     * Unmap mappedByteBuffer
     * See https://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java
     */
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

    public static String getString(final byte[] bs, final int off, final int len) {
        return new String(bs, off, len, StandardCharsets.UTF_8);
    }

    public static final String IPV6_START_MARK     = "[";

    public static final String IPV6_END_MARK       = "]";

    private static final int   IPV6_ADDRESS_LENGTH = 16;

    /**
     * check whether the ip address is IPv6.
     *
     * @param addr ip address
     * @return boolean
     */
    public static boolean isIPv6(final String addr) {
        try {
            return InetAddress.getByName(addr).getAddress().length == IPV6_ADDRESS_LENGTH;
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Parse peerId from string that generated by {@link #toString()}
     * This method can support parameter string values are below:
     *
     * <pre>
     * PeerId.parse("a:b")          = new PeerId("a", "b", 0 , -1)
     * PeerId.parse("a:b:c")        = new PeerId("a", "b", "c", -1)
     * PeerId.parse("a:b::d")       = new PeerId("a", "b", 0, "d")
     * PeerId.parse("a:b:c:d")      = new PeerId("a", "b", "c", "d")
     * </pre>
     */
    public static String[] parsePeerId(final String s) {
        if (s.startsWith(IPV6_START_MARK) && StringUtils.containsIgnoreCase(s, IPV6_END_MARK)) {
            String ipv6Addr;
            if (s.endsWith(IPV6_END_MARK)) {
                ipv6Addr = s;
            } else {
                ipv6Addr = s.substring(0, (s.indexOf(IPV6_END_MARK) + 1));
            }
            if (!isIPv6(ipv6Addr)) {
                throw new IllegalArgumentException("The IPv6 address(\"" + ipv6Addr + "\") is incorrect.");
            }
            String tempString = s.substring((s.indexOf(ipv6Addr) + ipv6Addr.length()));
            if (tempString.startsWith(":")) {
                tempString = tempString.substring(1);
            }
            String[] tempArr = StringUtils.splitPreserveAllTokens(tempString, ':');
            String[] result = new String[1 + tempArr.length];
            result[0] = ipv6Addr;
            System.arraycopy(tempArr, 0, result, 1, tempArr.length);
            return result;
        } else {
            return StringUtils.splitPreserveAllTokens(s, ':');
        }
    }
}
