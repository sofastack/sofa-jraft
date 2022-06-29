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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * ThreadPool based on Raft-Group isolation
 *
 * @author tynan.liu
 * @date 2022/6/24 17:25
 **/
public class ThreadPoolsFactory {
    private static final Logger                                    LOG                      = LoggerFactory
                                                                                                .getLogger(ThreadPoolsFactory.class);
    private static final ConcurrentMap<String, ThreadPoolExecutor> GROUP_THREAD_POOL_ROUTER = new ConcurrentHashMap<>();

    private static class GlobalThreadPoolHolder {
        private static volatile ThreadPoolExecutor DEFAULT_GLOBAL_THREAD_POOL;

        private GlobalThreadPoolHolder() {
            if (DEFAULT_GLOBAL_THREAD_POOL != null) {
                throw new IllegalStateException(
                    "DEFAULT_GLOBAL_THREAD_POOL has been created, please avoid using reflection to create this instance.");
            }
        }

        private static ThreadPoolExecutor getGlobalExecutor() {
            if (DEFAULT_GLOBAL_THREAD_POOL == null) {
                synchronized (GlobalThreadPoolHolder.class) {
                    if (DEFAULT_GLOBAL_THREAD_POOL == null) {
                        DEFAULT_GLOBAL_THREAD_POOL = ThreadPoolUtil.newBuilder()
                            .poolName("JRAFT_GROUP_DEFAULT_EXECUTOR").enableMetric(true)
                            .coreThreads(Utils.MIN_CLOSURE_EXECUTOR_POOL_SIZE)
                            .maximumThreads(Utils.MAX_CLOSURE_EXECUTOR_POOL_SIZE).keepAliveSeconds(60L)
                            .workQueue(new SynchronousQueue<>())
                            .threadFactory(new NamedThreadFactory("JRaft-Group-Default-Executor-", true)).build();
                    }
                }
            }
            return DEFAULT_GLOBAL_THREAD_POOL;
        }
    }

    /**
     * Register the default ThreadPoolExecutor
     * @param groupId Raft-Group
     */
    public static void registerThreadPool(String groupId) {
        registerThreadPool(groupId, GlobalThreadPoolHolder.getGlobalExecutor());
    }

    /**
     * You can specify the ThreadPoolExecutor yourself here
     * @param groupId Raft-Group
     * @param executor To specify ThreadPoolExecutor
     */
    public static void registerThreadPool(String groupId, ThreadPoolExecutor executor) {
        if (executor == null) {
            executor = GlobalThreadPoolHolder.getGlobalExecutor();
        }

        if (GROUP_THREAD_POOL_ROUTER.putIfAbsent(groupId, executor) != null) {
            LOG.warn("The group: {} has already registered the ThreadPool", groupId);
        }
    }

    protected static ThreadPoolExecutor getOrDefaultExecutor(String groupId) {
        if (StringUtils.isEmpty(groupId) || !GROUP_THREAD_POOL_ROUTER.containsKey(groupId)) {
            return GlobalThreadPoolHolder.getGlobalExecutor();
        }
        return GROUP_THREAD_POOL_ROUTER.get(groupId);
    }

    /**
     * Run a task in thread pool,returns the future object.
     */
    public static Future<?> runInThread(String groupId, final Runnable runnable) {
        return getOrDefaultExecutor(groupId).submit(runnable);
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
