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

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.MemoryDBOptions;
import com.alipay.sofa.jraft.rhea.storage.MemoryKVStoreSnapshotFile.SequenceDB;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.RegionHelper;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.codahale.metrics.Timer;

import static com.alipay.sofa.jraft.rhea.storage.MemoryKVStoreSnapshotFile.FencingKeyDB;
import static com.alipay.sofa.jraft.rhea.storage.MemoryKVStoreSnapshotFile.LockerDB;
import static com.alipay.sofa.jraft.rhea.storage.MemoryKVStoreSnapshotFile.Segment;
import static com.alipay.sofa.jraft.rhea.storage.MemoryKVStoreSnapshotFile.TailIndex;

/**
 * @author jiachun.fjc
 */
public class MemoryRawKVStore extends BatchRawKVStore<MemoryDBOptions> {

    private static final Logger                          LOG            = LoggerFactory
                                                                            .getLogger(MemoryRawKVStore.class);

    private static final String                          SEQUENCE_DB    = "sequenceDB";
    private static final String                          FENCING_KEY_DB = "fencingKeyDB";
    private static final String                          LOCKER_DB      = "lockerDB";
    private static final String                          SEGMENT        = "segment";
    private static final String                          TAIL_INDEX     = "tailIndex";

    private static final byte                            DELIMITER      = (byte) ',';
    private static final Comparator<byte[]>              COMPARATOR     = BytesUtil.getDefaultByteArrayComparator();

    private final ConcurrentNavigableMap<byte[], byte[]> defaultDB      = new ConcurrentSkipListMap<>(COMPARATOR);
    private final Map<ByteArray, Long>                   sequenceDB     = new ConcurrentHashMap<>();
    private final Map<ByteArray, Long>                   fencingKeyDB   = new ConcurrentHashMap<>();
    private final Map<ByteArray, DistributedLock.Owner>  lockerDB       = new ConcurrentHashMap<>();

    private volatile MemoryDBOptions                     opts;

    @Override
    public boolean init(final MemoryDBOptions opts) {
        this.opts = opts;
        LOG.info("[MemoryRawKVStore] start successfully, options: {}.", opts);
        return true;
    }

    @Override
    public void shutdown() {
        this.defaultDB.clear();
        this.sequenceDB.clear();
        this.fencingKeyDB.clear();
        this.lockerDB.clear();
    }

    @Override
    public KVIterator localIterator() {
        return new MemoryKVIterator(this.defaultDB);
    }

    @Override
    public void get(final byte[] key, @SuppressWarnings("unused") final boolean readOnlySafe,
                    final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("GET");
        try {
            final byte[] value = this.defaultDB.get(key);
            setSuccess(closure, value);
        } catch (final Exception e) {
            LOG.error("Fail to [GET], key: [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [GET]");
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void multiGet(final List<byte[]> keys, @SuppressWarnings("unused") final boolean readOnlySafe,
                         final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("MULTI_GET");
        try {
            final Map<ByteArray, byte[]> resultMap = Maps.newHashMap();
            for (final byte[] key : keys) {
                final byte[] value = this.defaultDB.get(key);
                if (value == null) {
                    continue;
                }
                resultMap.put(ByteArray.wrap(key), value);
            }
            setSuccess(closure, resultMap);
        } catch (final Exception e) {
            LOG.error("Fail to [MULTI_GET], key size: [{}], {}.", keys.size(), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [MULTI_GET]");
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void containsKey(final byte[] key, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("CONTAINS_KEY");
        try {
            final boolean exists = this.defaultDB.containsKey(key);
            setSuccess(closure, exists);
        } catch (final Exception e) {
            LOG.error("Fail to [CONTAINS_KEY], key: [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [CONTAINS_KEY]");
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit,
                     @SuppressWarnings("unused") final boolean readOnlySafe, final boolean returnValue,
                     final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("SCAN");
        final List<KVEntry> entries = Lists.newArrayList();
        final int maxCount = normalizeLimit(limit);
        final ConcurrentNavigableMap<byte[], byte[]> subMap;
        final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
        if (endKey == null) {
            subMap = this.defaultDB.tailMap(realStartKey);
        } else {
            subMap = this.defaultDB.subMap(realStartKey, endKey);
        }
        try {
            for (final Map.Entry<byte[], byte[]> entry : subMap.entrySet()) {
                entries.add(new KVEntry(entry.getKey(), returnValue ? entry.getValue() : null));
                if (entries.size() >= maxCount) {
                    break;
                }
            }
            setSuccess(closure, entries);
        } catch (final Exception e) {
            LOG.error("Fail to [SCAN], range: ['[{}, {})'], {}.", BytesUtil.toHex(startKey), BytesUtil.toHex(endKey),
                StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [SCAN]");
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit,
                            @SuppressWarnings("unused") final boolean readOnlySafe, final boolean returnValue,
                            final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("REVERSE_SCAN");
        final List<KVEntry> entries = Lists.newArrayList();
        final int maxCount = normalizeLimit(limit);
        final ConcurrentNavigableMap<byte[], byte[]> subMap;
        final byte[] realEndKey = BytesUtil.nullToEmpty(endKey);
        if (startKey == null) {
            subMap = this.defaultDB.descendingMap().headMap(realEndKey);
        } else {
            subMap = this.defaultDB.descendingMap().subMap(startKey, realEndKey);
        }
        try {
            for (final Map.Entry<byte[], byte[]> entry : subMap.entrySet()) {
                entries.add(new KVEntry(entry.getKey(), returnValue ? entry.getValue() : null));
                if (entries.size() >= maxCount) {
                    break;
                }
            }
            setSuccess(closure, entries);
        } catch (final Exception e) {
            LOG.error("Fail to [REVERSE_SCAN], range: ['[{}, {})'], {}.", BytesUtil.toHex(startKey),
                BytesUtil.toHex(endKey), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [REVERSE_SCAN]");
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void getSequence(final byte[] seqKey, final int step, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("GET_SEQUENCE");
        try {
            final ByteArray wrappedKey = ByteArray.wrap(seqKey);
            Long startVal = this.sequenceDB.get(wrappedKey);
            startVal = startVal == null ? 0 : startVal;
            if (step < 0) {
                // never get here
                setFailure(closure, "Fail to [GET_SEQUENCE], step must >= 0");
                return;
            }
            if (step == 0) {
                setSuccess(closure, new Sequence(startVal, startVal));
                return;
            }
            final long endVal = getSafeEndValueForSequence(startVal, step);
            if (startVal != endVal) {
                this.sequenceDB.put(wrappedKey, endVal);
            }
            setSuccess(closure, new Sequence(startVal, endVal));
        } catch (final Exception e) {
            LOG.error("Fail to [GET_SEQUENCE], [key = {}, step = {}], {}.", BytesUtil.toHex(seqKey), step,
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [GET_SEQUENCE]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void resetSequence(final byte[] seqKey, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("RESET_SEQUENCE");
        try {
            this.sequenceDB.remove(ByteArray.wrap(seqKey));
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [RESET_SEQUENCE], [key = {}], {}.", BytesUtil.toHex(seqKey),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [RESET_SEQUENCE]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void put(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("PUT");
        try {
            this.defaultDB.put(key, value);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [PUT], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void getAndPut(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("GET_PUT");
        try {
            final byte[] prevVal = this.defaultDB.put(key, value);
            setSuccess(closure, prevVal);
        } catch (final Exception e) {
            LOG.error("Fail to [GET_PUT], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [GET_PUT]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void compareAndPut(final byte[] key, final byte[] expect, final byte[] update, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("COMPARE_PUT");
        try {
            final byte[] actual = this.defaultDB.get(key);
            if (Arrays.equals(expect, actual)) {
                this.defaultDB.put(key, update);
                setSuccess(closure, Boolean.TRUE);
            } else {
                setSuccess(closure, Boolean.FALSE);
            }
        } catch (final Exception e) {
            LOG.error("Fail to [COMPARE_PUT], [{}, {}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(expect),
                BytesUtil.toHex(update), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [COMPARE_PUT]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void merge(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("MERGE");
        try {
            this.defaultDB.compute(key, (ignored, oldVal) -> {
                if (oldVal == null) {
                    return value;
                } else {
                    final byte[] newVal = new byte[oldVal.length + 1 + value.length];
                    System.arraycopy(oldVal, 0, newVal, 0, oldVal.length);
                    newVal[oldVal.length] = DELIMITER;
                    System.arraycopy(value, 0, newVal, oldVal.length + 1, value.length);
                    return newVal;
                }
            });
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [MERGE], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                    StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [MERGE]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void put(final List<KVEntry> entries, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("PUT_LIST");
        try {
            for (final KVEntry entry : entries) {
                this.defaultDB.put(entry.getKey(), entry.getValue());
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Failed to [PUT_LIST], [size = {}], {}.", entries.size(), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT_LIST]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void compareAndPutAll(final List<CASEntry> entries, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("COMPARE_PUT_ALL");
        try {
            for (final CASEntry entry : entries) {
                final byte[] actual = this.defaultDB.get(entry.getKey());
                if (!Arrays.equals(entry.getExpect(), actual)) {
                    setSuccess(closure, Boolean.FALSE);
                    return;
                }
            }

            for (final CASEntry entry : entries) {
                this.defaultDB.put(entry.getKey(), entry.getUpdate());
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Failed to [COMPARE_PUT_ALL], [size = {}], {}.", entries.size(), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [COMPARE_PUT_ALL]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void putIfAbsent(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("PUT_IF_ABSENT");
        try {
            final byte[] prevValue = this.defaultDB.putIfAbsent(key, value);
            setSuccess(closure, prevValue);
        } catch (final Exception e) {
            LOG.error("Fail to [PUT_IF_ABSENT], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT_IF_ABSENT]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void tryLockWith(final byte[] key, final byte[] fencingKey, final boolean keepLease,
                            final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("TRY_LOCK");
        try {
            // The algorithm relies on the assumption that while there is no
            // synchronized clock across the processes, still the local time in
            // every process flows approximately at the same rate, with an error
            // which is small compared to the auto-release time of the lock.
            final long now = acquirer.getLockingTimestamp();
            final long timeoutMillis = acquirer.getLeaseMillis();
            final ByteArray wrappedKey = ByteArray.wrap(key);
            final DistributedLock.Owner prevOwner = this.lockerDB.get(wrappedKey);

            final DistributedLock.Owner owner;
            // noinspection ConstantConditions
            do {
                final DistributedLock.OwnerBuilder builder = DistributedLock.newOwnerBuilder();
                if (prevOwner == null) {
                    // no others own this lock
                    if (keepLease) {
                        // it wants to keep the lease but too late, will return failure
                        owner = builder //
                            // set acquirer id
                            .id(acquirer.getId())
                            // fail to keep lease
                            .remainingMillis(DistributedLock.OwnerBuilder.KEEP_LEASE_FAIL)
                            // set failure
                            .success(false).build();
                        break;
                    }
                    // is first time to try lock (another possibility is that this lock has been deleted),
                    // will return successful
                    owner = builder //
                        // set acquirer id, now it will own the lock
                        .id(acquirer.getId())
                        // set a new deadline
                        .deadlineMillis(now + timeoutMillis)
                        // first time to acquire and success
                        .remainingMillis(DistributedLock.OwnerBuilder.FIRST_TIME_SUCCESS)
                        // create a new fencing token
                        .fencingToken(getNextFencingToken(fencingKey))
                        // init acquires
                        .acquires(1)
                        // set acquirer ctx
                        .context(acquirer.getContext())
                        // set successful
                        .success(true).build();
                    this.lockerDB.put(wrappedKey, owner);
                    break;
                }

                // this lock has an owner, check if it has expired
                final long remainingMillis = prevOwner.getDeadlineMillis() - now;
                if (remainingMillis < 0) {
                    // the previous owner is out of lease
                    if (keepLease) {
                        // it wants to keep the lease but too late, will return failure
                        owner = builder //
                            // still previous owner id
                            .id(prevOwner.getId())
                            // do not update
                            .deadlineMillis(prevOwner.getDeadlineMillis())
                            // fail to keep lease
                            .remainingMillis(DistributedLock.OwnerBuilder.KEEP_LEASE_FAIL)
                            // set previous ctx
                            .context(prevOwner.getContext())
                            // set failure
                            .success(false).build();
                        break;
                    }
                    // create new lock owner
                    owner = builder //
                        // set acquirer id, now it will own the lock
                        .id(acquirer.getId())
                        // set a new deadline
                        .deadlineMillis(now + timeoutMillis)
                        // success as a new acquirer
                        .remainingMillis(DistributedLock.OwnerBuilder.NEW_ACQUIRE_SUCCESS)
                        // create a new fencing token
                        .fencingToken(getNextFencingToken(fencingKey))
                        // init acquires
                        .acquires(1)
                        // set acquirer ctx
                        .context(acquirer.getContext())
                        // set successful
                        .success(true).build();
                    this.lockerDB.put(wrappedKey, owner);
                    break;
                }

                // the previous owner is not out of lease (remainingMillis >= 0)
                final boolean isReentrant = prevOwner.isSameAcquirer(acquirer);
                if (isReentrant) {
                    // is the same old friend come back (reentrant lock)
                    if (keepLease) {
                        // the old friend only wants to keep lease of lock
                        owner = builder //
                            // still previous owner id
                            .id(prevOwner.getId())
                            // update the deadline to keep lease
                            .deadlineMillis(now + timeoutMillis)
                            // success to keep lease
                            .remainingMillis(DistributedLock.OwnerBuilder.KEEP_LEASE_SUCCESS)
                            // keep fencing token
                            .fencingToken(prevOwner.getFencingToken())
                            // keep acquires
                            .acquires(prevOwner.getAcquires())
                            // do not update ctx when keeping lease
                            .context(prevOwner.getContext())
                            // set successful
                            .success(true).build();
                        this.lockerDB.put(wrappedKey, owner);
                        break;
                    }
                    // now we are sure that is an old friend who is back again (reentrant lock)
                    owner = builder //
                        // still previous owner id
                        .id(prevOwner.getId())
                        // by the way, the lease will also be kept
                        .deadlineMillis(now + timeoutMillis)
                        // success reentrant
                        .remainingMillis(DistributedLock.OwnerBuilder.REENTRANT_SUCCESS)
                        // keep fencing token
                        .fencingToken(prevOwner.getFencingToken())
                        // acquires++
                        .acquires(prevOwner.getAcquires() + 1)
                        // update ctx when reentrant
                        .context(acquirer.getContext())
                        // set successful
                        .success(true).build();
                    this.lockerDB.put(wrappedKey, owner);
                    break;
                }

                // the lock is exist and also prev locker is not the same as current
                owner = builder //
                    // set previous owner id to tell who is the real owner
                    .id(prevOwner.getId())
                    // set the remaining lease time of current owner
                    .remainingMillis(remainingMillis)
                    // set previous ctx
                    .context(prevOwner.getContext())
                    // set failure
                    .success(false).build();
                LOG.debug("Another locker [{}] is trying the existed lock [{}].", acquirer, prevOwner);
            } while (false);

            setSuccess(closure, owner);
        } catch (final Exception e) {
            LOG.error("Fail to [TRY_LOCK], [{}, {}], {}.", BytesUtil.toHex(key), acquirer, StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [TRY_LOCK]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void releaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("RELEASE_LOCK");
        try {
            final ByteArray wrappedKey = ByteArray.wrap(key);
            final DistributedLock.Owner prevOwner = this.lockerDB.get(wrappedKey);

            final DistributedLock.Owner owner;
            // noinspection ConstantConditions
            do {
                final DistributedLock.OwnerBuilder builder = DistributedLock.newOwnerBuilder();
                if (prevOwner == null) {
                    LOG.warn("Lock not exist: {}.", acquirer);
                    owner = builder //
                        // set acquirer id
                        .id(acquirer.getId())
                        // set acquirer fencing token
                        .fencingToken(acquirer.getFencingToken())
                        // set acquires=0
                        .acquires(0)
                        // set successful
                        .success(true).build();
                    break;
                }

                if (prevOwner.isSameAcquirer(acquirer)) {
                    final long acquires = prevOwner.getAcquires() - 1;
                    owner = builder //
                        // still previous owner id
                        .id(prevOwner.getId())
                        // do not update deadline
                        .deadlineMillis(prevOwner.getDeadlineMillis())
                        // keep fencing token
                        .fencingToken(prevOwner.getFencingToken())
                        // acquires--
                        .acquires(acquires)
                        // set previous ctx
                        .context(prevOwner.getContext())
                        // set successful
                        .success(true).build();
                    if (acquires <= 0) {
                        // real delete, goodbye ~
                        this.lockerDB.remove(wrappedKey);
                    } else {
                        // acquires--
                        this.lockerDB.put(wrappedKey, owner);
                    }
                    break;
                }

                // invalid acquirer, can't to release the lock
                owner = builder //
                    // set previous owner id to tell who is the real owner
                    .id(prevOwner.getId())
                    // keep previous fencing token
                    .fencingToken(prevOwner.getFencingToken())
                    // do not update acquires
                    .acquires(prevOwner.getAcquires())
                    // set previous ctx
                    .context(prevOwner.getContext())
                    // set failure
                    .success(false).build();
                LOG.warn("The lock owner is: [{}], [{}] could't release it.", prevOwner, acquirer);
            } while (false);

            setSuccess(closure, owner);
        } catch (final Exception e) {
            LOG.error("Fail to [RELEASE_LOCK], [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [RELEASE_LOCK]", e);
        } finally {
            timeCtx.stop();
        }
    }

    private long getNextFencingToken(final byte[] fencingKey) {
        final Timer.Context timeCtx = getTimeContext("FENCING_TOKEN");
        try {
            // Don't worry about the token number overflow.
            // It takes about 290,000 years for the 1 million TPS system
            // to use the numbers in the range [0 ~ Long.MAX_VALUE].
            final byte[] realKey = BytesUtil.nullToEmpty(fencingKey);
            return this.fencingKeyDB.compute(ByteArray.wrap(realKey), (key, prevVal) -> {
                if (prevVal == null) {
                    return 1L;
                }
                return ++prevVal;
            });
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void delete(final byte[] key, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("DELETE");
        try {
            this.defaultDB.remove(key);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [DELETE], [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void deleteRange(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("DELETE_RANGE");
        try {
            final ConcurrentNavigableMap<byte[], byte[]> subMap = this.defaultDB.subMap(startKey, endKey);
            if (!subMap.isEmpty()) {
                subMap.clear();
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [DELETE_RANGE], ['[{}, {})'], {}.", BytesUtil.toHex(startKey), BytesUtil.toHex(endKey),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE_RANGE]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void delete(final List<byte[]> keys, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("DELETE_LIST");
        try {
            for (final byte[] key : keys) {
                this.defaultDB.remove(key);
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Failed to [DELETE_LIST], [size = {}], {}.", keys.size(), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE_LIST]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public long getApproximateKeysInRange(final byte[] startKey, final byte[] endKey) {
        final Timer.Context timeCtx = getTimeContext("APPROXIMATE_KEYS");
        try {
            final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
            final ConcurrentNavigableMap<byte[], byte[]> subMap;
            if (endKey == null) {
                subMap = this.defaultDB.tailMap(realStartKey);
            } else {
                subMap = this.defaultDB.subMap(realStartKey, endKey);
            }
            return subMap.size();
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public byte[] jumpOver(final byte[] startKey, final long distance) {
        final Timer.Context timeCtx = getTimeContext("JUMP_OVER");
        try {
            final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
            final ConcurrentNavigableMap<byte[], byte[]> tailMap = this.defaultDB.tailMap(realStartKey);
            if (tailMap.isEmpty()) {
                return null;
            }
            long approximateKeys = 0;
            byte[] lastKey = null;
            for (final byte[] key : tailMap.keySet()) {
                lastKey = key;
                if (++approximateKeys >= distance) {
                    break;
                }
            }
            if (lastKey == null) {
                return null;
            }
            final byte[] endKey = new byte[lastKey.length];
            System.arraycopy(lastKey, 0, endKey, 0, lastKey.length);
            return endKey;
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void initFencingToken(final byte[] parentKey, final byte[] childKey) {
        final Timer.Context timeCtx = getTimeContext("INIT_FENCING_TOKEN");
        try {
            final byte[] realKey = BytesUtil.nullToEmpty(parentKey);
            final Long parentVal = this.fencingKeyDB.get(ByteArray.wrap(realKey));
            if (parentVal == null) {
                return;
            }
            this.fencingKeyDB.put(ByteArray.wrap(childKey), parentVal);
        } finally {
            timeCtx.stop();
        }
    }

    void doSnapshotSave(final MemoryKVStoreSnapshotFile snapshotFile, final String snapshotPath, final Region region)
                                                                                                                     throws Exception {
        final Timer.Context timeCtx = getTimeContext("SNAPSHOT_SAVE");
        try {
            final String tempPath = snapshotPath + "_temp";
            final File tempFile = new File(tempPath);
            FileUtils.deleteDirectory(tempFile);
            FileUtils.forceMkdir(tempFile);

            snapshotFile.writeToFile(tempPath, SEQUENCE_DB, new SequenceDB(subRangeMap(this.sequenceDB, region)));
            snapshotFile
                .writeToFile(tempPath, FENCING_KEY_DB, new FencingKeyDB(subRangeMap(this.fencingKeyDB, region)));
            snapshotFile.writeToFile(tempPath, LOCKER_DB, new LockerDB(subRangeMap(this.lockerDB, region)));
            final int size = this.opts.getKeysPerSegment();
            final List<Pair<byte[], byte[]>> segment = Lists.newArrayListWithCapacity(size);
            int index = 0;
            final byte[] realStartKey = BytesUtil.nullToEmpty(region.getStartKey());
            final byte[] endKey = region.getEndKey();
            final NavigableMap<byte[], byte[]> subMap;
            if (endKey == null) {
                subMap = this.defaultDB.tailMap(realStartKey);
            } else {
                subMap = this.defaultDB.subMap(realStartKey, endKey);
            }
            for (final Map.Entry<byte[], byte[]> entry : subMap.entrySet()) {
                segment.add(Pair.of(entry.getKey(), entry.getValue()));
                if (segment.size() >= size) {
                    snapshotFile.writeToFile(tempPath, SEGMENT + index++, new Segment(segment));
                    segment.clear();
                }
            }
            if (!segment.isEmpty()) {
                snapshotFile.writeToFile(tempPath, SEGMENT + index++, new Segment(segment));
                segment.clear();
            }
            snapshotFile.writeToFile(tempPath, TAIL_INDEX, new TailIndex(--index));

            final File destinationPath = new File(snapshotPath);
            FileUtils.deleteDirectory(destinationPath);
            FileUtils.moveDirectory(tempFile, destinationPath);
        } finally {
            timeCtx.stop();
        }
    }

    void doSnapshotLoad(final MemoryKVStoreSnapshotFile snapshotFile, final String snapshotPath) throws Exception {
        final Timer.Context timeCtx = getTimeContext("SNAPSHOT_LOAD");
        try {
            final SequenceDB sequenceDB = snapshotFile.readFromFile(snapshotPath, SEQUENCE_DB, SequenceDB.class);
            final FencingKeyDB fencingKeyDB = snapshotFile.readFromFile(snapshotPath, FENCING_KEY_DB,
                FencingKeyDB.class);
            final LockerDB lockerDB = snapshotFile.readFromFile(snapshotPath, LOCKER_DB, LockerDB.class);

            this.sequenceDB.putAll(sequenceDB.data());
            this.fencingKeyDB.putAll(fencingKeyDB.data());
            this.lockerDB.putAll(lockerDB.data());

            final TailIndex tailIndex = snapshotFile.readFromFile(snapshotPath, TAIL_INDEX, TailIndex.class);
            final int tail = tailIndex.data();
            final List<Segment> segments = Lists.newArrayListWithCapacity(tail + 1);
            for (int i = 0; i <= tail; i++) {
                final Segment segment = snapshotFile.readFromFile(snapshotPath, SEGMENT + i, Segment.class);
                segments.add(segment);
            }
            for (final Segment segment : segments) {
                for (final Pair<byte[], byte[]> p : segment.data()) {
                    this.defaultDB.put(p.getKey(), p.getValue());
                }
            }
        } finally {
            timeCtx.stop();
        }
    }

    static <V> Map<ByteArray, V> subRangeMap(final Map<ByteArray, V> input, final Region region) {
        if (RegionHelper.isSingleGroup(region)) {
            return input;
        }
        final Map<ByteArray, V> output = new HashMap<>();
        for (final Map.Entry<ByteArray, V> entry : input.entrySet()) {
            final ByteArray key = entry.getKey();
            if (RegionHelper.isKeyInRegion(key.getBytes(), region)) {
                output.put(key, entry.getValue());
            }
        }
        return output;
    }
}
