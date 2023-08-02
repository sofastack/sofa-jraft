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
package com.alipay.sofa.jraft.rhea.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.FollowerStateListener;
import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.StateListener;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;

/**
 * User layer KV store api.
 *
 * <pre>
 *                           ┌────────────────────────────┐
 *                           │                            │
 *                           │        RheaKVStore         │────────────────┐
 *                           │                            │                │
 *                           └────────────────────────────┘                ▼
 *                                        │ ▲               ┌────────────────────────────┐
 *                                        │ │               │   PlacementDriverClient    │
 *                                        │ │               └────────────────────────────┘
 *                                        │ │                              │
 *                                        │ │                              ▼
 *                                        │ │               ┌────────────────────────────┐
 *                                        │ │               │      RegionRouteTable      │
 *                                        │ │               └────────────────────────────┘
 *                    ┌───────────────────┘ │                              │
 *                    │                     │                              ▼
 *                    │                     │               ┌────────────────────────────┐
 *                    │                     └───────────────│        LoadBalancer        │
 *                  split                                   └────────────────────────────┘
 *                 request                             local
 *                    ├────────────────────────────────invoke──────────────────────────────────────────┐
 *                    │                                                                                │
 *                    ▼                                                                                │
 *     ┌────────────────────────────┐           ┌────────────────────────────┐                         │
 *     │      RheaKVRpcService      │───rpc────▶│     KVCommandProcessor     │                         │
 *     └────────────────────────────┘           └────────────────────────────┘                         │
 *                                                             │                                       │
 *                                                             ▼                                       ▼
 *                                              ┌────────────────────────────┐          ┌────────────────────────────┐
 *                                              │      RegionKVService       │────────▶ │     MetricsRawKVStore      │
 *                                              └────────────────────────────┘          └────────────────────────────┘
 *                                                                                                     │
 *                                                                                                     │
 *                                                                                                     ▼
 *     ┌────────────────────────────┐           ┌────────────────────────────┐          ┌────────────────────────────┐
 *     │      RocksRawKVStore       │◀──────────│    KVStoreStateMachine     │◀──raft───│       RaftRawKVStore       │
 *     └────────────────────────────┘           └────────────────────────────┘          └────────────────────────────┘
 * </pre>
 *
 * @author jiachun.fjc
 */
public interface RheaKVStore extends Lifecycle<RheaKVStoreOptions> {

    /**
     * Equivalent to {@code get(key, true)}.
     */
    CompletableFuture<byte[]> get(final byte[] key);

    /**
     * @see #get(byte[])
     */
    CompletableFuture<byte[]> get(final String key);

    /**
     * Get which returns a new byte array storing the value associated
     * with the specified input key if any.  null will be returned if
     * the specified key is not found.
     *
     * @param key          the key retrieve the value.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @return a byte array storing the value associated with the input key if
     * any.  null if it does not find the specified key.
     */
    CompletableFuture<byte[]> get(final byte[] key, final boolean readOnlySafe);

    /**
     * @see #get(byte[], boolean)
     */
    CompletableFuture<byte[]> get(final String key, final boolean readOnlySafe);

    /**
     * @see #get(byte[])
     */
    byte[] bGet(final byte[] key);

    /**
     * @see #get(String)
     */
    byte[] bGet(final String key);

    /**
     * @see #get(byte[], boolean)
     */
    byte[] bGet(final byte[] key, final boolean readOnlySafe);

    /**
     * @see #get(String, boolean)
     */
    byte[] bGet(final String key, final boolean readOnlySafe);

    /**
     * Equivalent to {@code multiGet(keys, true)}.
     */
    CompletableFuture<Map<ByteArray, byte[]>> multiGet(final List<byte[]> keys);

    /**
     * Returns a map of keys for which values were found in database.
     *
     * @param keys         list of keys for which values need to be retrieved.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @return a map where key of map is the key passed by user and value for map
     * entry is the corresponding value in database.
     */
    CompletableFuture<Map<ByteArray, byte[]>> multiGet(final List<byte[]> keys, final boolean readOnlySafe);

    /**
     * @see #multiGet(List)
     */
    Map<ByteArray, byte[]> bMultiGet(final List<byte[]> keys);

    /**
     * @see #multiGet(List, boolean)
     */
    Map<ByteArray, byte[]> bMultiGet(final List<byte[]> keys, final boolean readOnlySafe);

    /**
     * Returns whether database contains the specified input key.
     *
     * @param key the specified key database contains.
     * @return whether database contains the specified key.
     */
    CompletableFuture<Boolean> containsKey(final byte[] key);

    /**
     * @see #containsKey(byte[])
     */
    CompletableFuture<Boolean> containsKey(final String key);

    /**
     * @see #containsKey(byte[])
     */
    Boolean bContainsKey(final byte[] key);

    /**
     * @see #containsKey(byte[])
     */
    Boolean bContainsKey(final String key);

    /**
     * Equivalent to {@code scan(startKey, endKey, true)}.
     */
    CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey);

    /**
     * @see #scan(byte[], byte[])
     */
    CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey);

    /**
     * Equivalent to {@code scan(startKey, endKey, readOnlySafe, true)}.
     */
    CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe);

    /**
     * @see #scan(byte[], byte[], boolean)
     */
    CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey, final boolean readOnlySafe);

    /**
     * Query all data in the key of range [startKey, endKey).
     * <p>
     * Provide consistent reading if {@code readOnlySafe} is true.
     *
     * Scanning across multi regions maybe slower and devastating.
     *
     * @param startKey     first key to scan within database (included),
     *                     null means 'min-key' in the database.
     * @param endKey       last key to scan within database (excluded).
     *                     null means 'max-key' in the database.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @param returnValue  whether to return value.
     * @return a list where the key of range [startKey, endKey) passed by user
     * and value for {@code KVEntry}
     */
    CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                                          final boolean returnValue);

    /**
     * @see #scan(byte[], byte[], boolean, boolean)
     */
    CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey, final boolean readOnlySafe,
                                          final boolean returnValue);

    /**
     * @see #scan(byte[], byte[])
     */
    List<KVEntry> bScan(final byte[] startKey, final byte[] endKey);

    /**
     * @see #scan(String, String)
     */
    List<KVEntry> bScan(final String startKey, final String endKey);

    /**
     * @see #scan(String, String, boolean)
     */
    List<KVEntry> bScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe);

    /**
     * @see #scan(String, String, boolean)
     */
    List<KVEntry> bScan(final String startKey, final String endKey, final boolean readOnlySafe);

    /**
     * @see #scan(String, String, boolean, boolean)
     */
    List<KVEntry> bScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                        final boolean returnValue);

    /**
     * @see #scan(String, String, boolean, boolean)
     */
    List<KVEntry> bScan(final String startKey, final String endKey, final boolean readOnlySafe,
                        final boolean returnValue);

    /**
     * Equivalent to {@code reverseScan(startKey, endKey, true)}.
     */
    CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey);

    /**
     * @see #reverseScan(byte[], byte[])
     */
    CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey);

    /**
     * Equivalent to {@code reverseScan(startKey, endKey, readOnlySafe, true)}.
     */
    CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe);

    /**
     * @see #reverseScan(byte[], byte[], boolean)
     */
    CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey, final boolean readOnlySafe);

    /**
     * Reverse query all data in the key of range [startKey, endKey).
     * <p>
     * Provide consistent reading if {@code readOnlySafe} is true.
     *
     * Reverse scanning is usually much worse than forward scanning.
     *
     * Reverse scanning across multi regions maybe slower and devastating.
     *
     * @param startKey     first key to reverse scan within database (included),
     *                     null means 'max-key' in the database.
     * @param endKey       last key to reverse scan within database (excluded).
     *                     null means 'min-key' in the database.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @param returnValue  whether to return value.
     * @return a list where the key of range [startKey, endKey) passed by user
     * and value for {@code KVEntry}
     */
    CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey,
                                                 final boolean readOnlySafe, final boolean returnValue);

    /**
     * @see #reverseScan(byte[], byte[], boolean, boolean)
     */
    CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey,
                                                 final boolean readOnlySafe, final boolean returnValue);

    /**
     * @see #reverseScan(byte[], byte[])
     */
    List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey);

    /**
     * @see #scan(String, String)
     */
    List<KVEntry> bReverseScan(final String startKey, final String endKey);

    /**
     * @see #scan(String, String, boolean)
     */
    List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe);

    /**
     * @see #scan(String, String, boolean)
     */
    List<KVEntry> bReverseScan(final String startKey, final String endKey, final boolean readOnlySafe);

    /**
     * @see #reverseScan(String, String, boolean, boolean)
     */
    List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                               final boolean returnValue);

    /**
     * @see #reverseScan(String, String, boolean, boolean)
     */
    List<KVEntry> bReverseScan(final String startKey, final String endKey, final boolean readOnlySafe,
                               final boolean returnValue);

    /**
     * Equivalent to {@code iterator(startKey, endKey, bufSize, true)}.
     */
    RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize);

    /**
     * @see #iterator(byte[], byte[], int)
     */
    RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize);

    /**
     * Equivalent to {@code iterator(startKey, endKey, bufSize, true, true)}.
     */
    RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize,
                                   final boolean readOnlySafe);

    /**
     * @see #iterator(byte[], byte[], int, boolean)
     */
    RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize,
                                   final boolean readOnlySafe);

    /**
     * Returns a remote iterator over the contents of the database.
     *
     * Functionally similar to {@link #scan(byte[], byte[], boolean)},
     * but iterator only returns a small amount of data at a time, avoiding
     * a large amount of data returning to the client at one time causing
     * memory overflow, can think of it as a 'lazy scan' method.
     *
     * @param startKey     first key to scan within database (included),
     *                     null means 'min-key' in the database.
     * @param endKey       last key to scan within database (excluded),
     *                     null means 'max-key' in the database.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @param returnValue  whether to return value.
     * @return a iterator where the key of range [startKey, endKey) passed by
     * user and value for {@code KVEntry}
     */
    RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize,
                                   final boolean readOnlySafe, final boolean returnValue);

    /**
     * @see #iterator(byte[], byte[], int, boolean, boolean)
     */
    RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize,
                                   final boolean readOnlySafe, final boolean returnValue);

    /**
     * Get a globally unique auto-increment sequence.
     *
     * Be careful do not to try to get or update the value of {@code seqKey}
     * by other methods, you won't get it.
     *
     * @param seqKey the key of sequence
     * @param step   number of values obtained
     * @return a values range of [startValue, endValue)
     */
    CompletableFuture<Sequence> getSequence(final byte[] seqKey, final int step);

    /**
     * @see #getSequence(byte[], int)
     */
    CompletableFuture<Sequence> getSequence(final String seqKey, final int step);

    /**
     * @see #getSequence(byte[], int)
     */
    Sequence bGetSequence(final byte[] seqKey, final int step);

    /**
     * @see #getSequence(byte[], int)
     */
    Sequence bGetSequence(final String seqKey, final int step);

    /**
     * Gets the latest sequence start value, this is a read-only operation.
     *
     * Equivalent to {@code getSequence(seqKey, 0)}.
     *
     * @see #getSequence(byte[], int)
     *
     * @param seqKey the key of sequence
     * @return the latest sequence value
     */
    CompletableFuture<Long> getLatestSequence(final byte[] seqKey);

    /**
     * @see #getLatestSequence(byte[])
     */
    CompletableFuture<Long> getLatestSequence(final String seqKey);

    /**
     * @see #getLatestSequence(byte[])
     */
    Long bGetLatestSequence(final byte[] seqKey);

    /**
     * @see #getLatestSequence(byte[])
     */
    Long bGetLatestSequence(final String seqKey);

    /**
     * Reset the sequence to 0.
     *
     * @param seqKey the key of sequence
     */
    CompletableFuture<Boolean> resetSequence(final byte[] seqKey);

    /**
     * @see #resetSequence(byte[])
     */
    CompletableFuture<Boolean> resetSequence(final String seqKey);

    /**
     * @see #resetSequence(byte[])
     */
    Boolean bResetSequence(final byte[] seqKey);

    /**
     * @see #resetSequence(byte[])
     */
    Boolean bResetSequence(final String seqKey);

    /**
     * Set the database entry for "key" to "value".
     *
     * @param key   the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @return {@code true} if success.
     */
    CompletableFuture<Boolean> put(final byte[] key, final byte[] value);

    /**
     * @see #put(byte[], byte[])
     */
    CompletableFuture<Boolean> put(final String key, final byte[] value);

    /**
     * @see #put(byte[], byte[])
     */
    Boolean bPut(final byte[] key, final byte[] value);

    /**
     * @see #put(byte[], byte[])
     */
    Boolean bPut(final String key, final byte[] value);

    /**
     * Set the database entry for "key" to "value", and return the
     * previous value associated with "key", or null if there was no
     * mapping for "key".
     * @param key   the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @return the previous value associated with "key", or null if
     * there was no mapping for "key".
     */
    CompletableFuture<byte[]> getAndPut(final byte[] key, final byte[] value);

    /**
     * @see #getAndPut(byte[], byte[])
     */
    CompletableFuture<byte[]> getAndPut(final String key, final byte[] value);

    /**
     * @see #getAndPut(byte[], byte[])
     */
    byte[] bGetAndPut(final byte[] key, final byte[] value);

    /**
     * @see #getAndPut(byte[], byte[])
     */
    byte[] bGetAndPut(final String key, final byte[] value);

    /**
     * Atomically sets the value to the given updated value
     * if the current value equal (compare bytes) the expected value.
     *
     * @param key    the key retrieve the value
     * @param expect the expected value
     * @param update the new value
     * @return true if successful. False return indicates that the actual
     * value was not equal to the expected value.
     */
    CompletableFuture<Boolean> compareAndPut(final byte[] key, final byte[] expect, final byte[] update);

    /**
     * @see #compareAndPut(byte[], byte[], byte[])
     */
    CompletableFuture<Boolean> compareAndPut(final String key, final byte[] expect, final byte[] update);

    /**
     * @see #compareAndPut(byte[], byte[], byte[])
     */
    Boolean bCompareAndPut(final byte[] key, final byte[] expect, final byte[] update);

    /**
     * @see #compareAndPut(byte[], byte[], byte[])
     */
    Boolean bCompareAndPut(final String key, final byte[] expect, final byte[] update);

    /**
     * Add merge operand for key/value pair.
     *
     * <pre>
     *     // Writing aa under key
     *     db.put("key", "aa");
     *
     *     // Writing bb under key
     *     db.merge("key", "bb");
     *
     *     assertThat(db.get("key")).isEqualTo("aa,bb");
     * </pre>
     *
     * @param key   the specified key to be merged.
     * @param value the value to be merged with the current value for
     *              the specified key.
     * @return {@code true} if success.
     */
    CompletableFuture<Boolean> merge(final String key, final String value);

    /**
     * @see #merge(String, String)
     */
    Boolean bMerge(final String key, final String value);

    /**
     * The batch method of {@link #put(byte[], byte[])}
     */
    CompletableFuture<Boolean> put(final List<KVEntry> entries);

    /**
     * @see #put(List)
     */
    Boolean bPut(final List<KVEntry> entries);

    /**
     * The batch method of {@link #compareAndPut(byte[], byte[], byte[])}
     */
    CompletableFuture<Boolean> compareAndPutAll(final List<CASEntry> entries);

    /**
     * @see #compareAndPutAll(List)
     */
    Boolean bCompareAndPutAll(final List<CASEntry> entries);

    /**
     * If the specified key is not already associated with a value
     * associates it with the given value and returns {@code null},
     * else returns the current value.
     *
     * @param key   the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @return the previous value associated with the specified key,
     * or {@code null} if there was no mapping for the key.
     * (A {@code null} return can also indicate that the database.
     * previously associated {@code null} with the key.
     */
    CompletableFuture<byte[]> putIfAbsent(final byte[] key, final byte[] value);

    /**
     * @see #putIfAbsent(byte[], byte[])
     */
    CompletableFuture<byte[]> putIfAbsent(final String key, final byte[] value);

    /**
     * @see #putIfAbsent(byte[], byte[])
     */
    byte[] bPutIfAbsent(final byte[] key, final byte[] value);

    /**
     * @see #putIfAbsent(byte[], byte[])
     */
    byte[] bPutIfAbsent(final String key, final byte[] value);

    /**
     * Delete the database entry (if any) for "key".
     *
     * @param key key to delete within database.
     * @return {@code true} if success.
     */
    CompletableFuture<Boolean> delete(final byte[] key);

    /**
     * @see #delete(byte[])
     */
    CompletableFuture<Boolean> delete(final String key);

    /**
     * @see #delete(byte[])
     */
    Boolean bDelete(final byte[] key);

    /**
     * @see #delete(byte[])
     */
    Boolean bDelete(final String key);

    /**
     * Removes the database entries in the range ["startKey", "endKey"), i.e.,
     * including "startKey" and excluding "endKey".
     *
     * @param startKey first key to delete within database (included)
     * @param endKey   last key to delete within database (excluded)
     * @return {@code true} if success.
     */
    CompletableFuture<Boolean> deleteRange(final byte[] startKey, final byte[] endKey);

    /**
     * @see #deleteRange(byte[], byte[])
     */
    CompletableFuture<Boolean> deleteRange(final String startKey, final String endKey);

    /**
     * @see #deleteRange(byte[], byte[])
     */
    Boolean bDeleteRange(final byte[] startKey, final byte[] endKey);

    /**
     * @see #deleteRange(byte[], byte[])
     */
    Boolean bDeleteRange(final String startKey, final String endKey);

    /**
     * The batch method of {@link #delete(byte[])}
     */
    CompletableFuture<Boolean> delete(final List<byte[]> keys);

    /**
     * @see #delete(List)
     */
    Boolean bDelete(final List<byte[]> keys);

    /**
     * @see #getDistributedLock(byte[], long, TimeUnit, ScheduledExecutorService)
     */
    DistributedLock<byte[]> getDistributedLock(final byte[] target, final long lease, final TimeUnit unit);

    /**
     * @see #getDistributedLock(String, long, TimeUnit, ScheduledExecutorService)
     */
    DistributedLock<byte[]> getDistributedLock(final String target, final long lease, final TimeUnit unit);

    /**
     * Creates a distributed lock implementation that provides
     * exclusive access to a shared resource.
     * <p>
     * <pre>
     *      DistributedLock<byte[]> lock = ...;
     *      if (lock.tryLock()) {
     *          try {
     *              // manipulate protected state
     *          } finally {
     *              lock.unlock();
     *          }
     *      } else {
     *          // perform alternative actions
     *      }
     * </pre>
     *
     * The algorithm relies on the assumption that while there is no
     * synchronized clock across the processes, still the local time in
     * every process flows approximately at the same rate, with an error
     * which is small compared to the auto-release time of the lock.
     *
     * @param target   key of the distributed lock that acquired.
     * @param lease    the lease time for the distributed lock to live.
     * @param unit     the time unit of the {@code expire} argument.
     * @param watchdog if the watchdog is not null, it will auto keep
     *                 lease of current lock, otherwise won't keep lease,
     *                 this method dose not pay attention to the life cycle
     *                 of watchdog, please maintain it yourself.
     * @return a distributed lock instance.
     */
    DistributedLock<byte[]> getDistributedLock(final byte[] target, final long lease, final TimeUnit unit,
                                               final ScheduledExecutorService watchdog);

    /**
     * @see #getDistributedLock(byte[], long, TimeUnit, ScheduledExecutorService)
     */
    DistributedLock<byte[]> getDistributedLock(final String target, final long lease, final TimeUnit unit,
                                               final ScheduledExecutorService watchdog);

    /**
     * Returns current placement driver client instance.
     */
    PlacementDriverClient getPlacementDriverClient();

    /**
     * Add a listener for the state change of the leader with
     * this region.
     * <p>
     * In a special case, if that is a single region environment,
     * then regionId = -1 as the input parameter.
     */
    void addLeaderStateListener(final long regionId, final LeaderStateListener listener);

    /**
     * Add a listener for the state change of the follower with
     * this region.
     * <p>
     * In a special case, if that is a single region environment,
     * then regionId = -1 as the input parameter.
     */
    void addFollowerStateListener(final long regionId, final FollowerStateListener listener);

    /**
     * Add a listener for the state change (leader, follower) with
     * this region.
     * <p>
     * In a special case, if that is a single region environment,
     * then regionId = -1 as the input parameter.
     */
    void addStateListener(final long regionId, final StateListener listener);
}
