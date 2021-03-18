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

import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;

/**
 * Raw KV store
 *
 * @author dennis
 * @author jiachun.fjc
 */
public interface RawKVStore {

    /**
     * Returns a heap-allocated iterator over the contents of the
     * database.
     *
     * Caller should close the iterator when it is no longer needed.
     * The returned iterator should be closed before this db is closed.
     *
     * <pre>
     *     KVIterator it = unsafeLocalIterator();
     *     try {
     *         // do something
     *     } finally {
     *         it.close();
     *     }
     * <pre/>
     */
    KVIterator localIterator();

    /**
     * Equivalent to {@code get(key, true, closure)}.
     */
    void get(final byte[] key, final KVStoreClosure closure);

    /**
     * Get which returns a new byte array storing the value associated
     * with the specified input key if any.  null will be returned if
     * the specified key is not found.
     *
     * Provide consistent reading if {@code readOnlySafe} is true.
     */
    void get(final byte[] key, final boolean readOnlySafe, final KVStoreClosure closure);

    /**
     * Equivalent to {@code multiGet(keys, true, closure)}.
     */
    void multiGet(final List<byte[]> keys, final KVStoreClosure closure);

    /**
     * Returns a map of keys for which values were found in DB.
     *
     * The performance is similar to get(), multiGet() reads from the
     * same consistent view, but it is not faster.
     *
     * Provide consistent reading if {@code readOnlySafe} is true.
     */
    void multiGet(final List<byte[]> keys, final boolean readOnlySafe, final KVStoreClosure closure);

    /**
     * Returns whether DB contains the specified input key {@code key}.
     */
    void containsKey(final byte[] key, final KVStoreClosure closure);

    /**
     * Equivalent to {@code scan(startKey, endKey, Integer.MAX_VALUE, closure)}.
     */
    void scan(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure);

    /**
     * Equivalent to {@code scan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, closure)}.
     */
    void scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe, final KVStoreClosure closure);

    /**
     * Equivalent to {@code scan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, returnValue, closure)}.
     */
    void scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe, final boolean returnValue,
              final KVStoreClosure closure);

    /**
     * Equivalent to {@code scan(startKey, endKey, limit, true, closure)}.
     */
    void scan(final byte[] startKey, final byte[] endKey, final int limit, final KVStoreClosure closure);

    /**
     * Equivalent to {@code scan(startKey, endKey, limit, readOnlySafe, true, closure)}.
     */
    void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
              final KVStoreClosure closure);

    /**
     * Query all data in the key range of [startKey, endKey),
     * {@code limit} is the max number of keys.
     *
     * Provide consistent reading if {@code readOnlySafe} is true.
     *
     * Only return keys(ignore values) if {@code returnValue} is false.
     */
    void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
              final boolean returnValue, final KVStoreClosure closure);

    /**
     * Equivalent to {@code reverseScan(startKey, endKey, Integer.MAX_VALUE, closure)}.
     */
    void reverseScan(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure);

    /**
     * Equivalent to {@code reverseScan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, closure)}.
     */
    void reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                     final KVStoreClosure closure);

    /**
     * Equivalent to {@code reverseScan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, returnValue, closure)}.
     */
    void reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe, final boolean returnValue,
                     final KVStoreClosure closure);

    /**
     * Equivalent to {@code reverseScan(startKey, endKey, limit, true, closure)}.
     */
    void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final KVStoreClosure closure);

    /**
     * Equivalent to {@code reverseScan(startKey, endKey, limit, readOnlySafe, true, closure)}.
     */
    void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final KVStoreClosure closure);

    /**
     * Reverse query all data in the key range of [startKey, endKey),
     * {@code limit} is the max number of keys.
     *
     * Provide consistent reading if {@code readOnlySafe} is true.
     *
     * Only return keys(ignore values) if {@code returnValue} is false.
     */
    void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final boolean returnValue, final KVStoreClosure closure);

    /**
     * Get a globally unique auto-increment sequence.
     *
     * Be careful do not to try to get or update the value of {@code seqKey}
     * by other methods, you won't get it.
     */
    void getSequence(final byte[] seqKey, final int step, final KVStoreClosure closure);

    /**
     * Reset the sequence to 0.
     */
    void resetSequence(final byte[] seqKey, final KVStoreClosure closure);

    /**
     * Set the database entry for "key" to "value".
     */
    void put(final byte[] key, final byte[] value, final KVStoreClosure closure);

    /**
     * Set the database entry for "key" to "value", and return the
     * previous value associated with "key", or null if there was no
     * mapping for "key".
     */
    void getAndPut(final byte[] key, final byte[] value, final KVStoreClosure closure);

    /**
     * Atomically sets the value to the given updated value
     * if the current value equal (compare bytes) the expected value.
     */
    void compareAndPut(final byte[] key, final byte[] expect, final byte[] update, final KVStoreClosure closure);

    /**
     * Add merge operand for key/value pair.
     *
     *  <pre>
     *     // Writing aa under key
     *     db.put("key", "aa");
     *
     *     // Writing bb under key
     *     db.merge("key", "bb");
     *
     *     assertThat(db.get("key")).isEqualTo("aa,bb");
     * </pre>
     */
    void merge(final byte[] key, final byte[] value, final KVStoreClosure closure);

    /**
     * Store key/value pairs in batch.
     */
    void put(final List<KVEntry> entries, final KVStoreClosure closure);

    /**
     * CAS in batch.
     */
    void compareAndPutAll(final List<CASEntry> entries, final KVStoreClosure closure);

    /**
     * If the specified key is not already associated with a value
     * associates it with the given value and returns (closure.data)
     * {@code null}, else returns the current value.
     */
    void putIfAbsent(final byte[] key, final byte[] value, final KVStoreClosure closure);

    /**
     * Tries to lock the specified key, must contain a timeout
     */
    void tryLockWith(final byte[] key, final byte[] fencingKey, final boolean keepLease,
                     final DistributedLock.Acquirer acquirer, final KVStoreClosure closure);

    /**
     * Unlock the specified key with lock.
     */
    void releaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer, final KVStoreClosure closure);

    /**
     * Delete data by the {@code key}.
     */
    void delete(final byte[] key, final KVStoreClosure closure);

    /**
     * Delete all data in the key range of [startKey, endKey).
     */
    void deleteRange(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure);

    /**
     * Delete data by the {@code keys} in batch.
     */
    void delete(final List<byte[]> keys, final KVStoreClosure closure);

    /**
     * The {@code nodeExecutor} will be triggered when each node's
     * state machine is applied.
     */
    void execute(final NodeExecutor nodeExecutor, final boolean isLeader, final KVStoreClosure closure);
}
