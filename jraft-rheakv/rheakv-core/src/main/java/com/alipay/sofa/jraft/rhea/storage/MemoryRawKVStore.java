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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.options.MemoryDBOptions;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.codahale.metrics.Timer;

import static com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;

/**
 * @author jiachun.fjc
 */
public class MemoryRawKVStore extends BatchRawKVStore<MemoryDBOptions> {

    private static final Logger                    LOG             = LoggerFactory.getLogger(MemoryRawKVStore.class);

    private static final byte                      DELIMITER       = (byte) ',';
    private static final Comparator<byte[]>        COMPARATOR      = BytesUtil.getDefaultByteArrayComparator();

    // this rw_lock is for memory_kv_iterator
    private final ReadWriteLock                    readWriteLock   = new ReentrantReadWriteLock();

    private final AtomicLong                       databaseVersion = new AtomicLong(0);
    private final Serializer                       serializer      = Serializers.getDefault();

    private ConcurrentNavigableMap<byte[], byte[]> defaultDB;
    private Map<ByteArray, Long>                   sequenceDB;
    private Map<ByteArray, Long>                   fencingKeyDB;
    private Map<ByteArray, DistributedLock.Owner>  lockerDB;

    private MemoryDBOptions                        opts;

    @Override
    public boolean init(final MemoryDBOptions opts) {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            final ConcurrentNavigableMap<byte[], byte[]> defaultDB = new ConcurrentSkipListMap<>(COMPARATOR);
            final Map<ByteArray, Long> sequenceDB = Maps.newHashMap();
            final Map<ByteArray, Long> fencingKeyDB = Maps.newHashMap();
            final Map<ByteArray, DistributedLock.Owner> lockerDB = Maps.newHashMap();
            openMemoryDB(opts, defaultDB, sequenceDB, fencingKeyDB, lockerDB);
            LOG.info("[MemoryRawKVStore] start successfully, options: {}.", opts);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            closeMemoryDB();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public KVIterator localIterator() {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            return new MemoryKVIterator(this, this.defaultDB, readLock, this.databaseVersion.get());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void get(final byte[] key, @SuppressWarnings("unused") final boolean readOnlySafe,
                    final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("GET");
        try {
            final byte[] value = this.defaultDB.get(key);
            setSuccess(closure, value);
        } catch (final Exception e) {
            LOG.error("Fail to [GET], key: [{}], {}.", Arrays.toString(key), StackTraceUtil.stackTrace(e));
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
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("SCAN");
        final List<KVEntry> entries = Lists.newArrayList();
        // If limit == 0, it will be modified to Integer.MAX_VALUE on the server
        // and then queried.  So 'limit == 0' means that the number of queries is
        // not limited. This is because serialization uses varint to compress
        // numbers.  In the case of 0, only 1 byte is occupied, and Integer.MAX_VALUE
        // takes 5 bytes.
        final int maxCount = limit > 0 ? limit : Integer.MAX_VALUE;
        final ConcurrentNavigableMap<byte[], byte[]> subMap;
        final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
        if (endKey == null) {
            subMap = this.defaultDB.tailMap(realStartKey);
        } else {
            subMap = this.defaultDB.subMap(realStartKey, endKey);
        }
        try {
            for (final Map.Entry<byte[], byte[]> entry : subMap.entrySet()) {
                entries.add(new KVEntry(entry.getKey(), entry.getValue()));
                if (entries.size() >= maxCount) {
                    break;
                }
            }
            setSuccess(closure, entries);
        } catch (final Exception e) {
            LOG.error("Fail to [SCAN], range: ['[{}, {})'], {}.", Arrays.toString(startKey), Arrays.toString(endKey),
                StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [SCAN]");
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
            final long endVal = Math.max(startVal, (startVal + step) & Long.MAX_VALUE);
            this.sequenceDB.put(wrappedKey, endVal);
            setSuccess(closure, new Sequence(startVal, endVal));
        } catch (final Exception e) {
            LOG.error("Fail to [GET_SEQUENCE], [key = {}, step = {}], {}.", Arrays.toString(seqKey), step,
                StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [GET_SEQUENCE]");
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
            LOG.error("Fail to [RESET_SEQUENCE], [key = {}], {}.", Arrays.toString(seqKey),
                StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [RESET_SEQUENCE]");
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
            LOG.error("Fail to [PUT], [{}, {}], {}.", Arrays.toString(key), Arrays.toString(value),
                StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [PUT]");
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
            LOG.error("Fail to [GET_PUT], [{}, {}], {}.", Arrays.toString(key), Arrays.toString(value),
                StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [GET_PUT]");
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
            LOG.error("Fail to [MERGE], [{}, {}], {}.", Arrays.toString(key), Arrays.toString(value),
                    StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [MERGE]");
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
            setFailure(closure, "Fail to [PUT_LIST]");
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
            LOG.error("Fail to [PUT_IF_ABSENT], [{}, {}], {}.", Arrays.toString(key), Arrays.toString(value),
                StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [PUT_IF_ABSENT]");
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void tryLockWith(final byte[] key, final boolean keepLease, final DistributedLock.Acquirer acquirer,
                            final KVStoreClosure closure) {
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
                        .fencingToken(getNextFencingToken(LOCK_FENCING_KEY))
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
                        .fencingToken(getNextFencingToken(LOCK_FENCING_KEY))
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
            LOG.error("Fail to [TRY_LOCK], [{}, {}], {}.", Arrays.toString(key), acquirer, StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [TRY_LOCK]");
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
            LOG.error("Fail to [RELEASE_LOCK], [{}], {}.", Arrays.toString(key), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [RELEASE_LOCK]");
        } finally {
            timeCtx.stop();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private long getNextFencingToken(final byte[] fencingKey) {
        final Timer.Context timeCtx = getTimeContext("FENCING_TOKEN");
        try {
            // Don't worry about the token number overflow.
            // It takes about 290,000 years for the 1 million TPS system
            // to use the numbers in the range [0 ~ Long.MAX_VALUE].
            return this.fencingKeyDB.compute(ByteArray.wrap(fencingKey), (key, prevVal) -> {
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
            LOG.error("Fail to [DELETE], [{}], {}.", Arrays.toString(key), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [DELETE]");
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
            LOG.error("Fail to [DELETE_RANGE], ['[{}, {})'], {}.", Arrays.toString(startKey), Arrays.toString(endKey),
                StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [DELETE_RANGE]");
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
    public LocalFileMeta onSnapshotSave(final String snapshotPath) throws Exception {
        final File file = new File(snapshotPath);
        FileUtils.deleteDirectory(file);
        FileUtils.forceMkdir(file);
        writeToFile(snapshotPath, "sequenceDB", new SequenceDB(this.sequenceDB));
        writeToFile(snapshotPath, "fencingKeyDB", new FencingKeyDB(this.fencingKeyDB));
        writeToFile(snapshotPath, "lockerDB", new LockerDB(this.lockerDB));
        final int size = this.opts.getKeysPerSegment();
        final List<Pair<byte[], byte[]>> segment = Lists.newArrayListWithCapacity(size);
        int index = 0;
        for (final Map.Entry<byte[], byte[]> entry : this.defaultDB.entrySet()) {
            segment.add(Pair.of(entry.getKey(), entry.getValue()));
            if (segment.size() >= size) {
                writeToFile(snapshotPath, "segment" + index++, new Segment(segment));
                segment.clear();
            }
        }
        if (!segment.isEmpty()) {
            writeToFile(snapshotPath, "segment" + index++, new Segment(segment));
        }
        writeToFile(snapshotPath, "tailIndex", new TailIndex(--index));
        return null;
    }

    @Override
    public void onSnapshotLoad(final String snapshotPath, final LocalFileMeta meta) throws Exception {
        final File file = new File(snapshotPath);
        if (!file.exists()) {
            LOG.error("Snapshot file [{}] not exists.", snapshotPath);
            return;
        }
        final SequenceDB sequenceDB = readFromFile(snapshotPath, "sequenceDB", SequenceDB.class);
        final FencingKeyDB fencingKeyDB = readFromFile(snapshotPath, "fencingKeyDB", FencingKeyDB.class);
        final LockerDB lockerDB = readFromFile(snapshotPath, "lockerDB", LockerDB.class);
        final TailIndex tailIndex = readFromFile(snapshotPath, "tailIndex", TailIndex.class);
        final int tail = tailIndex.data();
        final List<Segment> segments = Lists.newArrayListWithCapacity(tail + 1);
        for (int i = 0; i <= tail; i++) {
            final Segment segment = readFromFile(snapshotPath, "segment" + i, Segment.class);
            segments.add(segment);
        }
        final ConcurrentNavigableMap<byte[], byte[]> defaultDB = new ConcurrentSkipListMap<>(COMPARATOR);
        for (final Segment segment : segments) {
            for (final Pair<byte[], byte[]> p : segment.data()) {
                defaultDB.put(p.getKey(), p.getValue());
            }
        }
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            closeMemoryDB();
            openMemoryDB(this.opts, defaultDB, sequenceDB.data(), fencingKeyDB.data(), lockerDB.data());
        } finally {
            writeLock.unlock();
        }
    }

    public long getDatabaseVersion() {
        return this.databaseVersion.get();
    }

    private <T> void writeToFile(final String rootPath, final String fileName, final Persistence<T> persist)
                                                                                                            throws Exception {
        final Path path = Paths.get(rootPath, fileName);
        try (final FileOutputStream out = new FileOutputStream(path.toFile());
                final BufferedOutputStream bufOutput = new BufferedOutputStream(out)) {
            final byte[] bytes = this.serializer.writeObject(persist);
            final byte[] lenBytes = new byte[4];
            Bits.putInt(lenBytes, 0, bytes.length);
            bufOutput.write(lenBytes);
            bufOutput.write(bytes);
            bufOutput.flush();
            out.getFD().sync();
        }
    }

    private <T> T readFromFile(final String rootPath, final String fileName, final Class<T> clazz) throws Exception {
        final Path path = Paths.get(rootPath, fileName);
        final File file = path.toFile();
        if (!file.exists()) {
            throw new NoSuchFieldException(path.toString());
        }
        try (final FileInputStream in = new FileInputStream(file);
                final BufferedInputStream bufInput = new BufferedInputStream(in)) {
            final byte[] lenBytes = new byte[4];
            int read = bufInput.read(lenBytes);
            if (read != lenBytes.length) {
                throw new IOException("fail to read snapshot file length, expects " + lenBytes.length
                                      + " bytes, but read " + read);
            }
            final int len = Bits.getInt(lenBytes, 0);
            final byte[] bytes = new byte[len];
            read = bufInput.read(bytes);
            if (read != bytes.length) {
                throw new IOException("fail to read snapshot file, expects " + bytes.length + " bytes, but read "
                                      + read);
            }
            return this.serializer.readObject(bytes, clazz);
        }
    }

    private void openMemoryDB(final MemoryDBOptions opts, final ConcurrentNavigableMap<byte[], byte[]> defaultDB,
                              final Map<ByteArray, Long> sequenceDB, final Map<ByteArray, Long> fencingKeyDB,
                              final Map<ByteArray, DistributedLock.Owner> lockerDB) {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            this.databaseVersion.incrementAndGet();
            this.opts = opts;
            this.defaultDB = defaultDB;
            this.sequenceDB = sequenceDB;
            this.fencingKeyDB = fencingKeyDB;
            this.lockerDB = lockerDB;
        } finally {
            writeLock.unlock();
        }
    }

    private void closeMemoryDB() {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            if (this.defaultDB != null) {
                this.defaultDB.clear();
            }
            if (this.sequenceDB != null) {
                this.sequenceDB.clear();
            }
            if (this.fencingKeyDB != null) {
                this.fencingKeyDB.clear();
            }
            if (this.lockerDB != null) {
                this.lockerDB.clear();
            }
        } finally {
            writeLock.unlock();
        }
    }

    static class Persistence<T> {

        private final T data;

        public Persistence(T data) {
            this.data = data;
        }

        public T data() {
            return data;
        }
    }

    /**
     * The data of sequences
     */
    static class SequenceDB extends Persistence<Map<ByteArray, Long>> {

        public SequenceDB(Map<ByteArray, Long> data) {
            super(data);
        }
    }

    /**
     * The data of fencing token keys
     */
    static class FencingKeyDB extends Persistence<Map<ByteArray, Long>> {

        public FencingKeyDB(Map<ByteArray, Long> data) {
            super(data);
        }
    }

    /**
     * The data of lock info
     */
    static class LockerDB extends Persistence<Map<ByteArray, DistributedLock.Owner>> {

        public LockerDB(Map<ByteArray, DistributedLock.Owner> data) {
            super(data);
        }
    }

    /**
     * The data will be cut into many small portions, each called a segment
     */
    static class Segment extends Persistence<List<Pair<byte[], byte[]>>> {

        public Segment(List<Pair<byte[], byte[]>> data) {
            super(data);
        }
    }

    /**
     * The 'tailIndex' records the largest segment number (the segment number starts from 0)
     */
    static class TailIndex extends Persistence<Integer> {

        public TailIndex(Integer data) {
            super(data);
        }
    }
}
