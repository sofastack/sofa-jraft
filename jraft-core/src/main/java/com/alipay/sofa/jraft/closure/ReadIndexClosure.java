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
package com.alipay.sofa.jraft.closure;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.alipay.sofa.jraft.util.timer.Timeout;
import com.alipay.sofa.jraft.util.timer.Timer;
import com.alipay.sofa.jraft.util.timer.TimerTask;

/**
 * Read index closure
 *
 * @author dennis
 */
public abstract class ReadIndexClosure implements Closure {

    private static final Logger                                      LOG               = LoggerFactory
                                                                                           .getLogger(ReadIndexClosure.class);

    private static final AtomicIntegerFieldUpdater<ReadIndexClosure> STATE_UPDATER     = AtomicIntegerFieldUpdater
                                                                                           .newUpdater(
                                                                                               ReadIndexClosure.class,
                                                                                               "state");

    private static final long                                        DEFAULT_TIMEOUT   = SystemPropertyUtil.getInt(
                                                                                           "jraft.read-index.timeout",
                                                                                           2 * 1000);

    private static final int                                         PENDING           = 0;
    private static final int                                         COMPLETE          = 1;
    private static final int                                         TIMEOUT           = 2;

    /**
     * Invalid log index -1.
     */
    public static final long                                         INVALID_LOG_INDEX = -1;

    private long                                                     index             = INVALID_LOG_INDEX;
    private byte[]                                                   requestContext;

    private volatile int                                             state             = PENDING;

    public ReadIndexClosure() {
        this(DEFAULT_TIMEOUT);
    }

    /**
     * Create a read-index closure with a timeout parameter.
     *
     * @param timeoutMs timeout millis
     */
    public ReadIndexClosure(long timeoutMs) {
        if (timeoutMs >= 0) {
            // Lazy to init the timer
            TimeoutScanner.TIMER.newTimeout(new TimeoutTask(this), timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Called when ReadIndex can be executed.
     *
     * @param status the readIndex status.
     * @param index  the committed index when starts readIndex.
     * @param reqCtx the request context passed by {@link Node#readIndex(byte[], ReadIndexClosure)}.
     * @see Node#readIndex(byte[], ReadIndexClosure)
     */
    public abstract void run(final Status status, final long index, final byte[] reqCtx);

    /**
     * Set callback result, called by jraft.
     *
     * @param index  the committed index.
     * @param reqCtx the request context passed by {@link Node#readIndex(byte[], ReadIndexClosure)}.
     */
    public void setResult(final long index, final byte[] reqCtx) {
        this.index = index;
        this.requestContext = reqCtx;
    }

    /**
     * The committed log index when starts readIndex request. return -1 if fails.
     *
     * @return returns the committed index.  returns -1 if fails.
     */
    public long getIndex() {
        return this.index;
    }

    /**
     * Returns the request context.
     *
     * @return the request context.
     */
    public byte[] getRequestContext() {
        return this.requestContext;
    }

    @Override
    public void run(final Status status) {
        if (!STATE_UPDATER.compareAndSet(this, PENDING, COMPLETE)) {
            LOG.warn("A timeout read-index response finally returned: {}.", status);
            return;
        }

        try {
            run(status, this.index, this.requestContext);
        } catch (final Throwable t) {
            LOG.error("Fail to run ReadIndexClosure with status: {}.", status, t);
        }
    }

    static class TimeoutTask implements TimerTask {

        private final ReadIndexClosure closure;

        TimeoutTask(ReadIndexClosure closure) {
            this.closure = closure;
        }

        @Override
        public void run(final Timeout timeout) throws Exception {
            if (!STATE_UPDATER.compareAndSet(this.closure, PENDING, TIMEOUT)) {
                return;
            }

            final Status status = new Status(RaftError.ETIMEDOUT, "read-index request timeout");
            try {
                this.closure.run(status, INVALID_LOG_INDEX, null);
            } catch (final Throwable t) {
                LOG.error("[Timeout] fail to run ReadIndexClosure with status: {}.", status, t);
            }
        }
    }

    /**
     * Lazy to create a timer
     */
    static class TimeoutScanner {
        private static final Timer TIMER = JRaftUtils.raftTimerFactory().createTimer("read-index.timeout.scanner");
    }
}
