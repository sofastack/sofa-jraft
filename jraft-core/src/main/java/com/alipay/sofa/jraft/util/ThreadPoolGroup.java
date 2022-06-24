package com.alipay.sofa.jraft.util;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * ThreadPool based on Raft-Group isolation
 *
 * @author tynan.liu
 * @date 2022/6/24 17:25
 **/
public class ThreadPoolGroup {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadPoolGroup.class);
    public static final ConcurrentMap<String, ThreadPoolExecutor> GROUP_THREAD_POOL_ROUTER = new ConcurrentHashMap<>();
    public static volatile ThreadPoolExecutor DEFAULT_GLOBAL_THREAD_POOL;

    public static void registerThreadPool(final MetricRegistry registry, String groupId, ThreadPoolExecutor executor) {
        if (GROUP_THREAD_POOL_ROUTER.containsKey(groupId)) {
            LOG.warn("The group:{} has already registered the ThreadPool", groupId);
            return;
        }

        synchronized (GROUP_THREAD_POOL_ROUTER) {
            if (GROUP_THREAD_POOL_ROUTER.containsKey(groupId)) {
                LOG.warn("The group:{} concurrently registers the ThreadPool", groupId);
                return;
            }
            if (executor == null) {
                if (DEFAULT_GLOBAL_THREAD_POOL == null) {
                    DEFAULT_GLOBAL_THREAD_POOL = ThreadPoolUtil.newBuilder().poolName("JRAFT_GROUP_DEFAULT_EXECUTOR")
                            .enableMetric(true).coreThreads(Utils.MIN_CLOSURE_EXECUTOR_POOL_SIZE)
                            .maximumThreads(Utils.MAX_CLOSURE_EXECUTOR_POOL_SIZE).keepAliveSeconds(60L)
                            .workQueue(new SynchronousQueue<>())
                            .threadFactory(new NamedThreadFactory("JRaft-Group-Default-Executor-", true)).build();
                }
                executor = DEFAULT_GLOBAL_THREAD_POOL;
            }
            GROUP_THREAD_POOL_ROUTER.putIfAbsent(groupId, executor);
            registerClosureExecutorMetrics(registry, groupId, executor);
        }
    }

    private static void registerClosureExecutorMetrics(final MetricRegistry registry, String groupId, final ThreadPoolExecutor executor) {
        registry.register(String.format("raft-group-%s-thread-pool", groupId)
                , new ThreadPoolMetricSet(executor));
    }

    /**
     * Run a task in thread pool,returns the future object.
     */
    public static Future<?> runInThread(String groupId, final Runnable runnable) {
        if (!GROUP_THREAD_POOL_ROUTER.containsKey(groupId)) {
            throw new IllegalArgumentException(String.format("The group: %s has not registered ThreadPool", groupId));
        }
        return GROUP_THREAD_POOL_ROUTER.get(groupId).submit(runnable);
    }

    /**
     * Run closure with status in thread pool.
     */
    public static Future<?> runClosureInThread(String groupId, final Closure done, final Status status) {
        if (done == null) {
            return null;
        }
        return runInThread(groupId, () -> {
            try {
                done.run(status);
            } catch (final Throwable t) {
                LOG.error("Fail to run done closure", t);
            }
        });
    }

    /**
     * Run closure with OK status in thread pool.
     */
    public static Future<?> runClosureInThread(String groupId, final Closure done) {
        if (done == null) {
            return null;
        }
        return runClosureInThread(groupId, done, Status.OK());
    }
}
