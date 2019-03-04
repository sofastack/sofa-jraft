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
package com.alipay.sofa.jraft.rhea.util.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.util.Requires;

/**
 * This is a ThreadFactory which assigns threads based the strategies provided.
 *
 * If no strategies are provided AffinityStrategies.ANY is used.
 *
 * @author jiachun.fjc
 */
public class AffinityNamedThreadFactory implements ThreadFactory {

    private static final Logger      LOG              = LoggerFactory.getLogger(AffinityNamedThreadFactory.class);

    private final AtomicInteger      id               = new AtomicInteger();
    private final String             name;
    private final boolean            daemon;
    private final int                priority;
    private final ThreadGroup        group;
    private final AffinityStrategy[] strategies;
    private AffinityLock             lastAffinityLock = null;

    public AffinityNamedThreadFactory(String name, AffinityStrategy... strategies) {
        this(name, false, Thread.NORM_PRIORITY, strategies);
    }

    public AffinityNamedThreadFactory(String name, boolean daemon, AffinityStrategy... strategies) {
        this(name, daemon, Thread.NORM_PRIORITY, strategies);
    }

    public AffinityNamedThreadFactory(String name, int priority, AffinityStrategy... strategies) {
        this(name, false, priority, strategies);
    }

    public AffinityNamedThreadFactory(String name, boolean daemon, int priority, AffinityStrategy... strategies) {
        this.name = "affinity." + name + " #";
        this.daemon = daemon;
        this.priority = priority;
        final SecurityManager s = System.getSecurityManager();
        this.group = (s == null) ? Thread.currentThread().getThreadGroup() : s.getThreadGroup();
        this.strategies = strategies.length == 0 ? new AffinityStrategy[] { AffinityStrategies.ANY } : strategies;
    }

    @SuppressWarnings("all")
    @Override
    public Thread newThread(final Runnable r) {
        Requires.requireNonNull(r, "runnable");

        final String name2 = this.name + this.id.getAndIncrement();
        final Runnable r2 = wrapRunnable(r);
        final Runnable r3 = new Runnable() {

            @Override
            public void run() {
                AffinityLock al = null;
                try {
                    al = acquireLockBasedOnLast();
                } catch (final Throwable ignored) {
                    // Defensive: ignored error on acquiring lock
                }
                try {
                    r2.run();
                } finally {
                    if (al != null) {
                        try {
                            al.release();
                        } catch (final Throwable ignored) {
                            // Defensive: ignored error on releasing lock
                        }
                    }
                }
            }
        };

        final Thread t = wrapThread(this.group, r3, name2);

        try {
            if (t.isDaemon() != this.daemon) {
                t.setDaemon(this.daemon);
            }

            if (t.getPriority() != this.priority) {
                t.setPriority(this.priority);
            }
        } catch (final Exception ignored) {
            // Doesn't matter even if failed to set.
        }

        LOG.info("Creates new {}.", t);

        return t;
    }

    public ThreadGroup getThreadGroup() {
        return group;
    }

    protected Runnable wrapRunnable(final Runnable r) {
        return r; // InternalThreadLocalRunnable.wrap(r)
    }

    protected Thread wrapThread(final ThreadGroup group, final Runnable r, final String name) {
        return new Thread(group, r, name);
    }

    private synchronized AffinityLock acquireLockBasedOnLast() {
        final AffinityLock al = this.lastAffinityLock == null ? AffinityLock.acquireLock() : lastAffinityLock
            .acquireLock(this.strategies);
        if (al.cpuId() >= 0) {
            if (!al.isBound()) {
                al.bind();
            }
            this.lastAffinityLock = al;
        }
        return al;
    }
}
