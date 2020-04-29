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
package com.alipay.sofa.jraft.rhea.storage;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.StorageException;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.codahale.metrics.Timer;

import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.DB_TIMER;

/**
 * @author jiachun.fjc
 */
public abstract class BaseRawKVStore<T> implements RawKVStore, Lifecycle<T> {

    @Override
    public void get(final byte[] key, final KVStoreClosure closure) {
        get(key, true, closure);
    }

    @Override
    public void multiGet(final List<byte[]> keys, final KVStoreClosure closure) {
        multiGet(keys, true, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe, final boolean returnValue,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, returnValue, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final KVStoreClosure closure) {
        scan(startKey, endKey, limit, true, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, limit, readOnlySafe, true, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        reverseScan(startKey, endKey, Integer.MAX_VALUE, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                            final KVStoreClosure closure) {
        reverseScan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                            final boolean returnValue, final KVStoreClosure closure) {
        reverseScan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, returnValue, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final KVStoreClosure closure) {
        reverseScan(startKey, endKey, limit, true, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                            final KVStoreClosure closure) {
        reverseScan(startKey, endKey, limit, readOnlySafe, true, closure);
    }

    @Override
    public void execute(final NodeExecutor nodeExecutor, final boolean isLeader, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("EXECUTE");
        try {
            if (nodeExecutor != null) {
                nodeExecutor.execute(Status.OK(), isLeader);
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            final Logger LOG = LoggerFactory.getLogger(getClass());
            LOG.error("Fail to [EXECUTE], {}.", StackTraceUtil.stackTrace(e));
            if (nodeExecutor != null) {
                nodeExecutor.execute(new Status(RaftError.EIO, "Fail to [EXECUTE]"), isLeader);
            }
            setCriticalError(closure, "Fail to [EXECUTE]", e);
        } finally {
            timeCtx.stop();
        }
    }

    public long getSafeEndValueForSequence(final long startVal, final int step) {
        return Math.max(startVal, Long.MAX_VALUE - step < startVal ? Long.MAX_VALUE : startVal + step);
    }

    /**
     * If limit == 0, it will be modified to Integer.MAX_VALUE on the server
     * and then queried.  So 'limit == 0' means that the number of queries is
     * not limited. This is because serialization uses varint to compress
     * numbers.  In the case of 0, only 1 byte is occupied, and Integer.MAX_VALUE
     * takes 5 bytes.
     *
     * @param limit input limit
     * @return normalize limit
     */
    protected int normalizeLimit(final int limit) {
        return limit > 0 ? limit : Integer.MAX_VALUE;
    }

    /**
     * Note: This is not a very precise behavior, don't rely on its accuracy.
     */
    public abstract long getApproximateKeysInRange(final byte[] startKey, final byte[] endKey);

    /**
     * Note: This is not a very precise behavior, don't rely on its accuracy.
     */
    public abstract byte[] jumpOver(final byte[] startKey, final long distance);

    /**
     * Init the fencing token of new region.
     *
     * @param parentKey the fencing key of parent region
     * @param childKey  the fencing key of new region
     */
    public abstract void initFencingToken(final byte[] parentKey, final byte[] childKey);

    // static methods
    //
    static Timer.Context getTimeContext(final String opName) {
        return KVMetrics.timer(DB_TIMER, opName).time();
    }

    /**
     * Sets success, if current node is a leader, reply to
     * client success with result data response.
     *
     * @param closure callback
     * @param data    result data to reply to client
     */
    static void setSuccess(final KVStoreClosure closure, final Object data) {
        if (closure != null) {
            // closure is null on follower node
            closure.setData(data);
            closure.run(Status.OK());
        }
    }

    /**
     * Sets failure, if current node is a leader, reply to
     * client failure response.
     *
     * @param closure callback
     * @param message error message
     */
    static void setFailure(final KVStoreClosure closure, final String message) {
        if (closure != null) {
            // closure is null on follower node
            closure.setError(Errors.STORAGE_ERROR);
            closure.run(new Status(RaftError.EIO, message));
        }
    }

    /**
     * Sets critical error and halt the state machine.
     *
     * If current node is a leader, first reply to client
     * failure response.
     *
     * @param closure callback
     * @param message error message
     * @param error   critical error
     */
    static void setCriticalError(final KVStoreClosure closure, final String message, final Throwable error) {
        // Will call closure#run in FSMCaller
        setClosureError(closure);
        if (error != null) {
            throw new StorageException(message, error);
        }
    }

    /**
     * Sets critical error and halt the state machine.
     *
     * If current node is a leader, first reply to client
     * failure response.
     *
     * @param closures callback list
     * @param message  error message
     * @param error    critical error
     */
    static void setCriticalError(final List<KVStoreClosure> closures, final String message, final Throwable error) {
        for (final KVStoreClosure closure : closures) {
            // Will call closure#run in FSMCaller
            setClosureError(closure);
        }
        if (error != null) {
            throw new StorageException(message, error);
        }
    }

    static void setClosureError(final KVStoreClosure closure) {
        if (closure != null) {
            // closure is null on follower node
            closure.setError(Errors.STORAGE_ERROR);
        }
    }

    /**
     * Sets the result first, then gets it by {@link #getData(KVStoreClosure)}
     * when we ensure successful.
     *
     * @param closure callback
     * @param data    data
     */
    static void setData(final KVStoreClosure closure, final Object data) {
        if (closure != null) {
            // closure is null on follower node
            closure.setData(data);
        }
    }

    /**
     * Gets data from closure.
     *
     * @param closure callback
     * @return closure data
     */
    static Object getData(final KVStoreClosure closure) {
        return closure == null ? null : closure.getData();
    }
}
