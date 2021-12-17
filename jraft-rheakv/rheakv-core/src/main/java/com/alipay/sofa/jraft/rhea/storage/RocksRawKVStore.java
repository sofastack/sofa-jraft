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
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupInfo;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.EnvOptions;
import org.rocksdb.Holder;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RestoreOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.SstFileWriter;
import org.rocksdb.Statistics;
import org.rocksdb.StatisticsCollectorCallback;
import org.rocksdb.StatsCollectorInput;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.errors.StorageException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.rocks.support.RocksStatisticsCollector;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.Partitions;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.DebugStatistics;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.StorageOptionsFactory;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.AdjustableSemaphore;
import com.codahale.metrics.Timer;

/**
 * Local KV store based on RocksDB
 *
 * @author dennis
 * @author jiachun.fjc
 */
public class RocksRawKVStore extends BatchRawKVStore<RocksDBOptions> implements Describer {

    private static final Logger                LOG                  = LoggerFactory.getLogger(RocksRawKVStore.class);

    static {
        RocksDB.loadLibrary();
    }

    // The maximum number of keys in once batch write
    public static final int                    MAX_BATCH_WRITE_SIZE = SystemPropertyUtil.getInt(
                                                                        "rhea.rocksdb.user.max_batch_write_size", 128);

    private final AdjustableSemaphore          shutdownLock         = new AdjustableSemaphore();
    private final ReadWriteLock                readWriteLock        = new ReentrantReadWriteLock();

    private final AtomicLong                   databaseVersion      = new AtomicLong(0);
    private final Serializer                   serializer           = Serializers.getDefault();

    private final List<ColumnFamilyOptions>    cfOptionsList        = Lists.newArrayList();
    private final List<ColumnFamilyDescriptor> cfDescriptors        = Lists.newArrayList();

    private ColumnFamilyHandle                 defaultHandle;
    private ColumnFamilyHandle                 sequenceHandle;
    private ColumnFamilyHandle                 lockingHandle;
    private ColumnFamilyHandle                 fencingHandle;

    private RocksDB                            db;

    private RocksDBOptions                     opts;
    private DBOptions                          options;
    private WriteOptions                       writeOptions;
    private DebugStatistics                    statistics;
    private RocksStatisticsCollector           statisticsCollector;

    @Override
    public boolean init(final RocksDBOptions opts) {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            if (this.db != null) {
                LOG.info("[RocksRawKVStore] already started.");
                return true;
            }
            this.opts = opts;
            this.options = createDBOptions();
            if (opts.isOpenStatisticsCollector()) {
                this.statistics = new DebugStatistics();
                this.options.setStatistics(this.statistics);
                final long intervalSeconds = opts.getStatisticsCallbackIntervalSeconds();
                if (intervalSeconds > 0) {
                    this.statisticsCollector = new RocksStatisticsCollector(TimeUnit.SECONDS.toMillis(intervalSeconds));
                    this.statisticsCollector.start();
                }
            }
            final ColumnFamilyOptions cfOptions = createColumnFamilyOptions();
            this.cfOptionsList.add(cfOptions);
            // default column family
            this.cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            // sequence column family
            this.cfDescriptors.add(new ColumnFamilyDescriptor(BytesUtil.writeUtf8("RHEA_SEQUENCE"), cfOptions));
            // locking column family
            this.cfDescriptors.add(new ColumnFamilyDescriptor(BytesUtil.writeUtf8("RHEA_LOCKING"), cfOptions));
            // fencing column family
            this.cfDescriptors.add(new ColumnFamilyDescriptor(BytesUtil.writeUtf8("RHEA_FENCING"), cfOptions));
            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(opts.isSync());
            // If `sync` is true, `disableWAL` must be set false.
            this.writeOptions.setDisableWAL(!opts.isSync() && opts.isDisableWAL());
            // Delete existing data, relying on raft's snapshot and log playback
            // to reply to the data is the correct behavior.
            destroyRocksDB(opts);
            openRocksDB(opts);
            this.shutdownLock.setMaxPermits(1);
            LOG.info("[RocksRawKVStore] start successfully, options: {}.", opts);
            return true;
        } catch (final Exception e) {
            LOG.error("Fail to open rocksDB at path {}, {}.", opts.getDbPath(), StackTraceUtil.stackTrace(e));
        } finally {
            writeLock.unlock();
        }
        return false;
    }

    @Override
    public void shutdown() {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            if (this.db == null) {
                return;
            }
            this.shutdownLock.setMaxPermits(0);
            closeRocksDB();
            if (this.defaultHandle != null) {
                this.defaultHandle.close();
                this.defaultHandle = null;
            }
            if (this.sequenceHandle != null) {
                this.sequenceHandle.close();
                this.sequenceHandle = null;
            }
            if (this.lockingHandle != null) {
                this.lockingHandle.close();
                this.lockingHandle = null;
            }
            if (this.fencingHandle != null) {
                this.fencingHandle.close();
                this.fencingHandle = null;
            }
            for (final ColumnFamilyOptions cfOptions : this.cfOptionsList) {
                cfOptions.close();
            }
            this.cfOptionsList.clear();
            this.cfDescriptors.clear();
            if (this.options != null) {
                this.options.close();
                this.options = null;
            }
            if (this.statisticsCollector != null) {
                try {
                    this.statisticsCollector.shutdown(3000);
                } catch (final Throwable ignored) {
                    // ignored
                }
            }
            if (this.statistics != null) {
                this.statistics.close();
                this.statistics = null;
            }
            if (this.writeOptions != null) {
                this.writeOptions.close();
                this.writeOptions = null;
            }
        } finally {
            writeLock.unlock();
            LOG.info("[RocksRawKVStore] shutdown successfully.");
        }
    }

    @Override
    public KVIterator localIterator() {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            return new RocksKVIterator(this, this.db.newIterator(), readLock, getDatabaseVersion());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void get(final byte[] key, @SuppressWarnings("unused") final boolean readOnlySafe,
                    final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("GET");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final byte[] value = this.db.get(key);
            setSuccess(closure, value);
        } catch (final Exception e) {
            LOG.error("Fail to [GET], key: [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [GET]");
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void multiGet(final List<byte[]> keys, @SuppressWarnings("unused") final boolean readOnlySafe,
                         final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("MULTI_GET");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final Map<byte[], byte[]> rawMap = this.db.multiGet(keys);
            final Map<ByteArray, byte[]> resultMap = Maps.newHashMapWithExpectedSize(rawMap.size());
            for (final Map.Entry<byte[], byte[]> entry : rawMap.entrySet()) {
                resultMap.put(ByteArray.wrap(entry.getKey()), entry.getValue());
            }
            setSuccess(closure, resultMap);
        } catch (final Exception e) {
            LOG.error("Fail to [MULTI_GET], key size: [{}], {}.", keys.size(), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [MULTI_GET]");
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void containsKey(final byte[] key, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("CONTAINS_KEY");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            boolean exists = false;
            Holder<byte[]> valueHolder = new Holder<>();
            if (this.db.keyMayExist(key, valueHolder)) {
                exists = valueHolder.getValue() != null;
            }
            setSuccess(closure, exists);
        } catch (final Exception e) {
            LOG.error("Fail to [CONTAINS_KEY], key: [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [CONTAINS_KEY]");
        } finally {
            readLock.unlock();
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
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try (final RocksIterator it = this.db.newIterator()) {
            if (startKey == null) {
                it.seekToFirst();
            } else {
                it.seek(startKey);
            }
            int count = 0;
            while (it.isValid() && count++ < maxCount) {
                final byte[] key = it.key();
                if (endKey != null && BytesUtil.compare(key, endKey) >= 0) {
                    break;
                }
                entries.add(new KVEntry(key, returnValue ? it.value() : null));
                it.next();
            }
            setSuccess(closure, entries);
        } catch (final Exception e) {
            LOG.error("Fail to [SCAN], range: ['[{}, {})'], {}.", BytesUtil.toHex(startKey), BytesUtil.toHex(endKey),
                StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [SCAN]");
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit,
                            @SuppressWarnings("unused") final boolean readOnlySafe, final boolean returnValue,
                            final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("REVERSE_SCAN");
        final List<KVEntry> entries = Lists.newArrayList();
        int maxCount = normalizeLimit(limit);
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try (final RocksIterator it = this.db.newIterator()) {
            if (startKey == null) {
                it.seekToLast();
            } else {
                it.seekForPrev(startKey);
            }
            int count = 0;
            while (it.isValid() && count++ < maxCount) {
                final byte[] key = it.key();
                if (endKey != null && BytesUtil.compare(key, endKey) <= 0) {
                    break;
                }
                entries.add(new KVEntry(key, returnValue ? it.value() : null));
                it.prev();
            }
            setSuccess(closure, entries);
        } catch (final Exception e) {
            LOG.error("Fail to [REVERSE_SCAN], range: ['[{}, {})'], {}.", BytesUtil.toHex(startKey),
                BytesUtil.toHex(endKey), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [REVERSE_SCAN]");
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void getSequence(final byte[] seqKey, final int step, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("GET_SEQUENCE");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final byte[] prevBytesVal = this.db.get(this.sequenceHandle, seqKey);
            long startVal;
            if (prevBytesVal == null) {
                startVal = 0;
            } else {
                startVal = Bits.getLong(prevBytesVal, 0);
            }
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
                final byte[] newBytesVal = new byte[8];
                Bits.putLong(newBytesVal, 0, endVal);
                this.db.put(this.sequenceHandle, this.writeOptions, seqKey, newBytesVal);
            }
            setSuccess(closure, new Sequence(startVal, endVal));
        } catch (final Exception e) {
            LOG.error("Fail to [GET_SEQUENCE], [key = {}, step = {}], {}.", BytesUtil.toHex(seqKey), step,
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [GET_SEQUENCE]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void resetSequence(final byte[] seqKey, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("RESET_SEQUENCE");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            this.db.delete(this.sequenceHandle, seqKey);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [RESET_SEQUENCE], [key = {}], {}.", BytesUtil.toHex(seqKey),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [RESET_SEQUENCE]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void batchResetSequence(final KVStateOutputList kvStates) {
        if (kvStates.isSingletonList()) {
            final KVState kvState = kvStates.getSingletonElement();
            resetSequence(kvState.getOp().getKey(), kvState.getDone());
            return;
        }
        final Timer.Context timeCtx = getTimeContext("BATCH_RESET_SEQUENCE");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            Partitions.manyToOne(kvStates, MAX_BATCH_WRITE_SIZE, (Function<List<KVState>, Void>) segment -> {
                try (final WriteBatch batch = new WriteBatch()) {
                    for (final KVState kvState : segment) {
                        batch.delete(sequenceHandle, kvState.getOp().getKey());
                    }
                    this.db.write(this.writeOptions, batch);
                    for (final KVState kvState : segment) {
                        setSuccess(kvState.getDone(), Boolean.TRUE);
                    }
                } catch (final Exception e) {
                    LOG.error("Failed to [BATCH_RESET_SEQUENCE], [size = {}], {}.", segment.size(),
                        StackTraceUtil.stackTrace(e));
                    setCriticalError(Lists.transform(kvStates, KVState::getDone), "Fail to [BATCH_RESET_SEQUENCE]", e);
                }
                return null;
            });
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void put(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("PUT");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            this.db.put(this.writeOptions, key, value);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [PUT], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void batchPut(final KVStateOutputList kvStates) {
        if (kvStates.isSingletonList()) {
            final KVState kvState = kvStates.getSingletonElement();
            final KVOperation op = kvState.getOp();
            put(op.getKey(), op.getValue(), kvState.getDone());
            return;
        }
        final Timer.Context timeCtx = getTimeContext("BATCH_PUT");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            Partitions.manyToOne(kvStates, MAX_BATCH_WRITE_SIZE, (Function<List<KVState>, Void>) segment -> {
                try (final WriteBatch batch = new WriteBatch()) {
                    for (final KVState kvState : segment) {
                        final KVOperation op = kvState.getOp();
                        batch.put(op.getKey(), op.getValue());
                    }
                    this.db.write(this.writeOptions, batch);
                    for (final KVState kvState : segment) {
                        setSuccess(kvState.getDone(), Boolean.TRUE);
                    }
                } catch (final Exception e) {
                    LOG.error("Failed to [BATCH_PUT], [size = {}] {}.", segment.size(), StackTraceUtil.stackTrace(e));
                    setCriticalError(Lists.transform(kvStates, KVState::getDone), "Fail to [BATCH_PUT]", e);
                }
                return null;
            });
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void getAndPut(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("GET_PUT");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final byte[] prevVal = this.db.get(key);
            this.db.put(this.writeOptions, key, value);
            setSuccess(closure, prevVal);
        } catch (final Exception e) {
            LOG.error("Fail to [GET_PUT], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [GET_PUT]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void batchGetAndPut(final KVStateOutputList kvStates) {
        if (kvStates.isSingletonList()) {
            final KVState kvState = kvStates.getSingletonElement();
            final KVOperation op = kvState.getOp();
            getAndPut(op.getKey(), op.getValue(), kvState.getDone());
            return;
        }
        final Timer.Context timeCtx = getTimeContext("BATCH_GET_PUT");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            Partitions.manyToOne(kvStates, MAX_BATCH_WRITE_SIZE, (Function<List<KVState>, Void>) segment -> {
                try (final WriteBatch batch = new WriteBatch()) {
                    final List<byte[]> keys = Lists.newArrayListWithCapacity(segment.size());
                    for (final KVState kvState : segment) {
                        final KVOperation op = kvState.getOp();
                        final byte[] key = op.getKey();
                        keys.add(key);
                        batch.put(key, op.getValue());
                    }
                    // first, get prev values
                    final Map<byte[], byte[]> prevValMap = this.db.multiGet(keys);
                    this.db.write(this.writeOptions, batch);
                    for (final KVState kvState : segment) {
                        setSuccess(kvState.getDone(), prevValMap.get(kvState.getOp().getKey()));
                    }
                } catch (final Exception e) {
                    LOG.error("Failed to [BATCH_GET_PUT], [size = {}] {}.", segment.size(),
                        StackTraceUtil.stackTrace(e));
                    setCriticalError(Lists.transform(kvStates, KVState::getDone), "Fail to [BATCH_GET_PUT]", e);
                }
                return null;
            });
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void compareAndPut(final byte[] key, final byte[] expect, final byte[] update, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("COMPARE_PUT");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final byte[] actual = this.db.get(key);
            if (Arrays.equals(expect, actual)) {
                this.db.put(this.writeOptions, key, update);
                setSuccess(closure, Boolean.TRUE);
            } else {
                setSuccess(closure, Boolean.FALSE);
            }
        } catch (final Exception e) {
            LOG.error("Fail to [COMPARE_PUT], [{}, {}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(expect),
                BytesUtil.toHex(update), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [COMPARE_PUT]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void batchCompareAndPut(final KVStateOutputList kvStates) {
        if (kvStates.isSingletonList()) {
            final KVState kvState = kvStates.getSingletonElement();
            final KVOperation op = kvState.getOp();
            compareAndPut(op.getKey(), op.getExpect(), op.getValue(), kvState.getDone());
            return;
        }
        final Timer.Context timeCtx = getTimeContext("BATCH_COMPARE_PUT");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            Partitions.manyToOne(kvStates, MAX_BATCH_WRITE_SIZE, (Function<List<KVState>, Void>) segment -> {
                try (final WriteBatch batch = new WriteBatch()) {
                    final Map<byte[], byte[]> expects = Maps.newHashMapWithExpectedSize(segment.size());
                    final Map<byte[], byte[]> updates = Maps.newHashMapWithExpectedSize(segment.size());
                    for (final KVState kvState : segment) {
                        final KVOperation op = kvState.getOp();
                        final byte[] key = op.getKey();
                        final byte[] expect = op.getExpect();
                        final byte[] update = op.getValue();
                        expects.put(key, expect);
                        updates.put(key, update);
                    }
                    final Map<byte[], byte[]> prevValMap = this.db.multiGet(Lists.newArrayList(expects.keySet()));
                    for (final KVState kvState : segment) {
                        final byte[] key = kvState.getOp().getKey();
                        if (Arrays.equals(expects.get(key), prevValMap.get(key))) {
                            batch.put(key, updates.get(key));
                            setData(kvState.getDone(), Boolean.TRUE);
                        } else {
                            setData(kvState.getDone(), Boolean.FALSE);
                        }
                    }
                    if (batch.count() > 0) {
                        this.db.write(this.writeOptions, batch);
                    }
                    for (final KVState kvState : segment) {
                        setSuccess(kvState.getDone(), getData(kvState.getDone()));
                    }
                } catch (final Exception e) {
                    LOG.error("Failed to [BATCH_COMPARE_PUT], [size = {}] {}.", segment.size(),
                        StackTraceUtil.stackTrace(e));
                    setCriticalError(Lists.transform(kvStates, KVState::getDone), "Fail to [BATCH_COMPARE_PUT]", e);
                }
                return null;
            });
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void merge(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("MERGE");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            this.db.merge(this.writeOptions, key, value);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [MERGE], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [MERGE]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void batchMerge(final KVStateOutputList kvStates) {
        if (kvStates.isSingletonList()) {
            final KVState kvState = kvStates.getSingletonElement();
            final KVOperation op = kvState.getOp();
            merge(op.getKey(), op.getValue(), kvState.getDone());
            return;
        }
        final Timer.Context timeCtx = getTimeContext("BATCH_MERGE");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            Partitions.manyToOne(kvStates, MAX_BATCH_WRITE_SIZE, (Function<List<KVState>, Void>) segment -> {
                try (final WriteBatch batch = new WriteBatch()) {
                    for (final KVState kvState : segment) {
                        final KVOperation op = kvState.getOp();
                        batch.merge(op.getKey(), op.getValue());
                    }
                    this.db.write(this.writeOptions, batch);
                    for (final KVState kvState : segment) {
                        setSuccess(kvState.getDone(), Boolean.TRUE);
                    }
                } catch (final Exception e) {
                    LOG.error("Failed to [BATCH_MERGE], [size = {}] {}.", segment.size(), StackTraceUtil.stackTrace(e));
                    setCriticalError(Lists.transform(kvStates, KVState::getDone), "Fail to [BATCH_MERGE]", e);
                }
                return null;
            });
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void put(final List<KVEntry> entries, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("PUT_LIST");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try (final WriteBatch batch = new WriteBatch()) {
            for (final KVEntry entry : entries) {
                batch.put(entry.getKey(), entry.getValue());
            }
            this.db.write(this.writeOptions, batch);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Failed to [PUT_LIST], [size = {}], {}.", entries.size(), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT_LIST]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void compareAndPutAll(final List<CASEntry> entries, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("COMPARE_PUT_ALL");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try (final WriteBatch batch = new WriteBatch()) {
            final List<byte[]> keys = Lists.newArrayList();
            for (final CASEntry entry : entries) {
                keys.add(entry.getKey());
            }
            final Map<byte[], byte[]> prevValMap = this.db.multiGet(keys);
            for (final CASEntry entry : entries) {
                if (!Arrays.equals(entry.getExpect(), prevValMap.get(entry.getKey()))) {
                    setSuccess(closure, Boolean.FALSE);
                    return;
                }
            }

            for (final CASEntry entry : entries) {
                batch.put(entry.getKey(), entry.getUpdate());
            }
            if (batch.count() > 0) {
                this.db.write(this.writeOptions, batch);
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Failed to [COMPARE_PUT_ALL], [size = {}] {}.", entries.size(), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [COMPARE_PUT_ALL]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void putIfAbsent(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("PUT_IF_ABSENT");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final byte[] prevVal = this.db.get(key);
            if (prevVal == null) {
                this.db.put(this.writeOptions, key, value);
            }
            setSuccess(closure, prevVal);
        } catch (final Exception e) {
            LOG.error("Fail to [PUT_IF_ABSENT], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT_IF_ABSENT]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void batchPutIfAbsent(final KVStateOutputList kvStates) {
        if (kvStates.isSingletonList()) {
            final KVState kvState = kvStates.getSingletonElement();
            final KVOperation op = kvState.getOp();
            putIfAbsent(op.getKey(), op.getValue(), kvState.getDone());
            return;
        }
        final Timer.Context timeCtx = getTimeContext("BATCH_PUT_IF_ABSENT");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            Partitions.manyToOne(kvStates, MAX_BATCH_WRITE_SIZE, (Function<List<KVState>, Void>) segment -> {
                try (final WriteBatch batch = new WriteBatch()) {
                    final List<byte[]> keys = Lists.newArrayListWithCapacity(segment.size());
                    final Map<byte[], byte[]> values = Maps.newHashMapWithExpectedSize(segment.size());
                    for (final KVState kvState : segment) {
                        final KVOperation op = kvState.getOp();
                        final byte[] key = op.getKey();
                        final byte[] value = op.getValue();
                        keys.add(key);
                        values.put(key, value);
                    }
                    final Map<byte[], byte[]> prevValMap = this.db.multiGet(keys);
                    for (final KVState kvState : segment) {
                        final byte[] key = kvState.getOp().getKey();
                        final byte[] prevVal = prevValMap.get(key);
                        if (prevVal == null) {
                            batch.put(key, values.get(key));
                        }
                        setData(kvState.getDone(), prevVal);
                    }
                    if (batch.count() > 0) {
                        this.db.write(this.writeOptions, batch);
                    }
                    for (final KVState kvState : segment) {
                        setSuccess(kvState.getDone(), getData(kvState.getDone()));
                    }
                } catch (final Exception e) {
                    LOG.error("Failed to [BATCH_PUT_IF_ABSENT], [size = {}] {}.", segment.size(),
                        StackTraceUtil.stackTrace(e));
                    setCriticalError(Lists.transform(kvStates, KVState::getDone), "Fail to [BATCH_PUT_IF_ABSENT]", e);
                }
                return null;
            });
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void tryLockWith(final byte[] key, final byte[] fencingKey, final boolean keepLease,
                            final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("TRY_LOCK");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            // The algorithm relies on the assumption that while there is no
            // synchronized clock across the processes, still the local time in
            // every process flows approximately at the same rate, with an error
            // which is small compared to the auto-release time of the lock.
            final long now = acquirer.getLockingTimestamp();
            final long timeoutMillis = acquirer.getLeaseMillis();
            final byte[] prevBytesVal = this.db.get(this.lockingHandle, key);

            final DistributedLock.Owner owner;
            // noinspection ConstantConditions
            do {
                final DistributedLock.OwnerBuilder builder = DistributedLock.newOwnerBuilder();
                if (prevBytesVal == null) {
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
                    this.db.put(this.lockingHandle, this.writeOptions, key, this.serializer.writeObject(owner));
                    break;
                }

                // this lock has an owner, check if it has expired
                final DistributedLock.Owner prevOwner = this.serializer.readObject(prevBytesVal,
                    DistributedLock.Owner.class);
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
                    this.db.put(this.lockingHandle, this.writeOptions, key, this.serializer.writeObject(owner));
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
                        this.db.put(this.lockingHandle, this.writeOptions, key, this.serializer.writeObject(owner));
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
                    this.db.put(this.lockingHandle, this.writeOptions, key, this.serializer.writeObject(owner));
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
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void releaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("RELEASE_LOCK");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final byte[] prevBytesVal = this.db.get(this.lockingHandle, key);

            final DistributedLock.Owner owner;
            // noinspection ConstantConditions
            do {
                final DistributedLock.OwnerBuilder builder = DistributedLock.newOwnerBuilder();
                if (prevBytesVal == null) {
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

                final DistributedLock.Owner prevOwner = this.serializer.readObject(prevBytesVal,
                    DistributedLock.Owner.class);

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
                        this.db.delete(this.lockingHandle, this.writeOptions, key);
                    } else {
                        // acquires--
                        this.db.put(this.lockingHandle, this.writeOptions, key, this.serializer.writeObject(owner));
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
            readLock.unlock();
            timeCtx.stop();
        }
    }

    private long getNextFencingToken(final byte[] fencingKey) throws RocksDBException {
        final Timer.Context timeCtx = getTimeContext("FENCING_TOKEN");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final byte[] realKey = BytesUtil.nullToEmpty(fencingKey);
            final byte[] prevBytesVal = this.db.get(this.fencingHandle, realKey);
            final long prevVal;
            if (prevBytesVal == null) {
                prevVal = 0; // init
            } else {
                prevVal = Bits.getLong(prevBytesVal, 0);
            }
            // Don't worry about the token number overflow.
            // It takes about 290,000 years for the 1 million TPS system
            // to use the numbers in the range [0 ~ Long.MAX_VALUE].
            final long newVal = prevVal + 1;
            final byte[] newBytesVal = new byte[8];
            Bits.putLong(newBytesVal, 0, newVal);
            this.db.put(this.fencingHandle, this.writeOptions, realKey, newBytesVal);
            return newVal;
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void delete(final byte[] key, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("DELETE");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            this.db.delete(this.writeOptions, key);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [DELETE], [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void batchDelete(final KVStateOutputList kvStates) {
        if (kvStates.isSingletonList()) {
            final KVState kvState = kvStates.getSingletonElement();
            delete(kvState.getOp().getKey(), kvState.getDone());
            return;
        }
        final Timer.Context timeCtx = getTimeContext("BATCH_DELETE");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            Partitions.manyToOne(kvStates, MAX_BATCH_WRITE_SIZE, (Function<List<KVState>, Void>) segment -> {
                try (final WriteBatch batch = new WriteBatch()) {
                    for (final KVState kvState : segment) {
                        batch.delete(kvState.getOp().getKey());
                    }
                    this.db.write(this.writeOptions, batch);
                    for (final KVState kvState : segment) {
                        setSuccess(kvState.getDone(), Boolean.TRUE);
                    }
                } catch (final Exception e) {
                    LOG.error("Failed to [BATCH_DELETE], [size = {}], {}.", segment.size(),
                        StackTraceUtil.stackTrace(e));
                    setCriticalError(Lists.transform(kvStates, KVState::getDone), "Fail to [BATCH_DELETE]", e);
                }
                return null;
            });
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void deleteRange(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("DELETE_RANGE");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            this.db.deleteRange(this.writeOptions, startKey, endKey);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [DELETE_RANGE], ['[{}, {})'], {}.", BytesUtil.toHex(startKey), BytesUtil.toHex(endKey),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE_RANGE]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void delete(final List<byte[]> keys, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("DELETE_LIST");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try (final WriteBatch batch = new WriteBatch()) {
            for (final byte[] key : keys) {
                batch.delete(key);
            }
            this.db.write(this.writeOptions, batch);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Failed to [DELETE_LIST], [size = {}], {}.", keys.size(), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE_LIST]", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public long getApproximateKeysInRange(final byte[] startKey, final byte[] endKey) {
        // TODO This is a sad code, the performance is too damn bad
        final Timer.Context timeCtx = getTimeContext("APPROXIMATE_KEYS");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        final Snapshot snapshot = this.db.getSnapshot();
        try (final ReadOptions readOptions = new ReadOptions()) {
            readOptions.setSnapshot(snapshot);
            try (final RocksIterator it = this.db.newIterator(readOptions)) {
                if (startKey == null) {
                    it.seekToFirst();
                } else {
                    it.seek(startKey);
                }
                long approximateKeys = 0;
                for (;;) {
                    // The accuracy is 100, don't ask more
                    for (int i = 0; i < 100; i++) {
                        if (!it.isValid()) {
                            return approximateKeys;
                        }
                        it.next();
                        ++approximateKeys;
                    }
                    if (endKey != null && BytesUtil.compare(it.key(), endKey) >= 0) {
                        return approximateKeys;
                    }
                }
            }
        } finally {
            // Nothing to release, rocksDB never own the pointer for a snapshot.
            snapshot.close();
            // The pointer to the snapshot is released by the database instance.
            this.db.releaseSnapshot(snapshot);
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public byte[] jumpOver(final byte[] startKey, final long distance) {
        final Timer.Context timeCtx = getTimeContext("JUMP_OVER");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        final Snapshot snapshot = this.db.getSnapshot();
        try (final ReadOptions readOptions = new ReadOptions()) {
            readOptions.setSnapshot(snapshot);
            try (final RocksIterator it = this.db.newIterator(readOptions)) {
                if (startKey == null) {
                    it.seekToFirst();
                } else {
                    it.seek(startKey);
                }
                long approximateKeys = 0;
                for (;;) {
                    byte[] lastKey = null;
                    if (it.isValid()) {
                        lastKey = it.key();
                    }
                    // The accuracy is 100, don't ask more
                    for (int i = 0; i < 100; i++) {
                        if (!it.isValid()) {
                            return lastKey;
                        }
                        it.next();
                        if (++approximateKeys >= distance) {
                            return it.key();
                        }
                    }
                }
            }
        } finally {
            // Nothing to release, rocksDB never own the pointer for a snapshot.
            snapshot.close();
            // The pointer to the snapshot is released by the database instance.
            this.db.releaseSnapshot(snapshot);
            readLock.unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void initFencingToken(final byte[] parentKey, final byte[] childKey) {
        final Timer.Context timeCtx = getTimeContext("INIT_FENCING_TOKEN");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final byte[] realKey = BytesUtil.nullToEmpty(parentKey);
            final byte[] parentBytesVal = this.db.get(this.fencingHandle, realKey);
            if (parentBytesVal == null) {
                return;
            }
            this.db.put(this.fencingHandle, this.writeOptions, childKey, parentBytesVal);
        } catch (final RocksDBException e) {
            throw new StorageException("Fail to init fencing token.", e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    public long getDatabaseVersion() {
        return this.databaseVersion.get();
    }

    public void addStatisticsCollectorCallback(final StatisticsCollectorCallback callback) {
        final RocksStatisticsCollector collector = Requires.requireNonNull(this.statisticsCollector,
            "statisticsCollector");
        final Statistics statistics = Requires.requireNonNull(this.statistics, "statistics");
        collector.addStatsCollectorInput(new StatsCollectorInput(statistics, callback));
    }

    boolean isFastSnapshot() {
        return Requires.requireNonNull(this.opts, "opts").isFastSnapshot();
    }

    boolean isAsyncSnapshot() {
        return Requires.requireNonNull(this.opts, "opts").isAsyncSnapshot();
    }

    CompletableFuture<Void> createSstFiles(final EnumMap<SstColumnFamily, File> sstFileTable, final byte[] startKey,
                                           final byte[] endKey, final ExecutorService executor) {
        final Snapshot snapshot;
        final CompletableFuture<Void> sstFuture = new CompletableFuture<>();
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            snapshot = this.db.getSnapshot();
            if (!isAsyncSnapshot()) {
                doCreateSstFiles(snapshot, sstFileTable, startKey, endKey, sstFuture);
                return sstFuture;
            }
        } finally {
            readLock.unlock();
        }

        // async snapshot
        executor.execute(() -> doCreateSstFiles(snapshot, sstFileTable, startKey, endKey, sstFuture));
        return sstFuture;
    }

    void doCreateSstFiles(final Snapshot snapshot, final EnumMap<SstColumnFamily, File> sstFileTable,
                          final byte[] startKey, final byte[] endKey, final CompletableFuture<Void> future) {
        final Timer.Context timeCtx = getTimeContext("CREATE_SST_FILE");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            if (!this.shutdownLock.isAvailable()) {
                // KV store has shutdown, we do not release rocksdb's snapshot
                future.completeExceptionally(new StorageException("KV store has shutdown."));
                return;
            }
            try (final ReadOptions readOptions = new ReadOptions();
                    final EnvOptions envOptions = new EnvOptions();
                    final Options options = new Options().setMergeOperator(new StringAppendOperator())) {
                readOptions.setSnapshot(snapshot);
                for (final Map.Entry<SstColumnFamily, File> entry : sstFileTable.entrySet()) {
                    final SstColumnFamily sstColumnFamily = entry.getKey();
                    final File sstFile = entry.getValue();
                    final ColumnFamilyHandle columnFamilyHandle = findColumnFamilyHandle(sstColumnFamily);
                    try (final RocksIterator it = this.db.newIterator(columnFamilyHandle, readOptions);
                            final SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options)) {
                        if (startKey == null) {
                            it.seekToFirst();
                        } else {
                            it.seek(startKey);
                        }
                        sstFileWriter.open(sstFile.getAbsolutePath());
                        long count = 0;
                        for (;;) {
                            if (!it.isValid()) {
                                break;
                            }
                            final byte[] key = it.key();
                            if (endKey != null && BytesUtil.compare(key, endKey) >= 0) {
                                break;
                            }
                            sstFileWriter.put(key, it.value());
                            ++count;
                            it.next();
                        }
                        if (count == 0) {
                            sstFileWriter.close();
                        } else {
                            sstFileWriter.finish();
                        }
                        LOG.info("Finish sst file {} with {} keys.", sstFile, count);
                    } catch (final RocksDBException e) {
                        throw new StorageException("Fail to create sst file at path: " + sstFile, e);
                    }
                }
                future.complete(null);
            } catch (final Throwable t) {
                future.completeExceptionally(t);
            } finally {
                // Nothing to release, rocksDB never own the pointer for a snapshot.
                snapshot.close();
                // The pointer to the snapshot is released by the database instance.
                this.db.releaseSnapshot(snapshot);
            }
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    void ingestSstFiles(final EnumMap<SstColumnFamily, File> sstFileTable) {
        final Timer.Context timeCtx = getTimeContext("INGEST_SST_FILE");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            for (final Map.Entry<SstColumnFamily, File> entry : sstFileTable.entrySet()) {
                final SstColumnFamily sstColumnFamily = entry.getKey();
                final File sstFile = entry.getValue();
                final ColumnFamilyHandle columnFamilyHandle = findColumnFamilyHandle(sstColumnFamily);
                try (final IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions()) {
                    if (FileUtils.sizeOf(sstFile) == 0L) {
                        return;
                    }
                    final String filePath = sstFile.getAbsolutePath();
                    LOG.info("Start ingest sst file {}.", filePath);
                    this.db.ingestExternalFile(columnFamilyHandle, Collections.singletonList(filePath), ingestOptions);
                } catch (final RocksDBException e) {
                    throw new StorageException("Fail to ingest sst file at path: " + sstFile, e);
                }
            }
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    RocksDBBackupInfo backupDB(final String backupDBPath) throws IOException {
        final Timer.Context timeCtx = getTimeContext("BACKUP_DB");
        FileUtils.forceMkdir(new File(backupDBPath));
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try (final BackupableDBOptions backupOpts = createBackupDBOptions(backupDBPath);
             final BackupEngine backupEngine = BackupEngine.open(this.options.getEnv(), backupOpts)) {
            backupEngine.createNewBackup(this.db, true);
            final List<BackupInfo> backupInfoList = backupEngine.getBackupInfo();
            if (backupInfoList.isEmpty()) {
                LOG.warn("Fail to backup at {}, empty backup info.", backupDBPath);
                return null;
            }
            // chose the backupInfo who has max backupId
            final BackupInfo backupInfo = Collections.max(backupInfoList, Comparator.comparingInt(BackupInfo::backupId));
            final RocksDBBackupInfo rocksBackupInfo = new RocksDBBackupInfo(backupInfo);
            LOG.info("Backup rocksDB into {} with backupInfo {}.", backupDBPath, rocksBackupInfo);
            return rocksBackupInfo;
        } catch (final RocksDBException e) {
            throw new StorageException("Fail to backup at path: " + backupDBPath, e);
        } finally {
            writeLock.unlock();
            timeCtx.stop();
        }
    }

    void restoreBackup(final String backupDBPath, final RocksDBBackupInfo rocksBackupInfo) {
        final Timer.Context timeCtx = getTimeContext("RESTORE_BACKUP");
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        closeRocksDB();
        try (final BackupableDBOptions backupOpts = createBackupDBOptions(backupDBPath);
                final BackupEngine backupEngine = BackupEngine.open(this.options.getEnv(), backupOpts);
                final RestoreOptions restoreOpts = new RestoreOptions(false)) {
            final String dbPath = this.opts.getDbPath();
            backupEngine.restoreDbFromBackup(rocksBackupInfo.getBackupId(), dbPath, dbPath, restoreOpts);
            LOG.info("Restored rocksDB from {} with {}.", backupDBPath, rocksBackupInfo);
            // reopen the db
            openRocksDB(this.opts);
        } catch (final RocksDBException e) {
            throw new StorageException("Fail to restore from path: " + backupDBPath, e);
        } finally {
            writeLock.unlock();
            timeCtx.stop();
        }
    }

    void writeSnapshot(final String snapshotPath) {
        final Timer.Context timeCtx = getTimeContext("WRITE_SNAPSHOT");
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try (final Checkpoint checkpoint = Checkpoint.create(this.db)) {
            final String tempPath = snapshotPath + "_temp";
            final File tempFile = new File(tempPath);
            FileUtils.deleteDirectory(tempFile);
            checkpoint.createCheckpoint(tempPath);
            final File snapshotFile = new File(snapshotPath);
            FileUtils.deleteDirectory(snapshotFile);
            if (!Utils.atomicMoveFile(tempFile, snapshotFile, true)) {
                throw new StorageException("Fail to rename [" + tempPath + "] to [" + snapshotPath + "].");
            }
        } catch (final StorageException e) {
            throw e;
        } catch (final Exception e) {
            throw new StorageException("Fail to write snapshot at path: " + snapshotPath, e);
        } finally {
            writeLock.unlock();
            timeCtx.stop();
        }
    }

    void readSnapshot(final String snapshotPath) {
        final Timer.Context timeCtx = getTimeContext("READ_SNAPSHOT");
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            final File snapshotFile = new File(snapshotPath);
            if (!snapshotFile.exists()) {
                LOG.error("Snapshot file [{}] not exists.", snapshotPath);
                return;
            }
            closeRocksDB();
            final String dbPath = this.opts.getDbPath();
            final File dbFile = new File(dbPath);
            FileUtils.deleteDirectory(dbFile);
            if (!Utils.atomicMoveFile(snapshotFile, dbFile, true)) {
                throw new StorageException("Fail to rename [" + snapshotPath + "] to [" + dbPath + "].");
            }
            // reopen the db
            openRocksDB(this.opts);
        } catch (final Exception e) {
            throw new StorageException("Fail to read snapshot from path: " + snapshotPath, e);
        } finally {
            writeLock.unlock();
            timeCtx.stop();
        }
    }

    CompletableFuture<Void> writeSstSnapshot(final String snapshotPath, final Region region, final ExecutorService executor) {
        final Timer.Context timeCtx = getTimeContext("WRITE_SST_SNAPSHOT");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final String tempPath = snapshotPath + "_temp";
            final File tempFile = new File(tempPath);
            FileUtils.deleteDirectory(tempFile);
            FileUtils.forceMkdir(tempFile);

            final EnumMap<SstColumnFamily, File> sstFileTable = getSstFileTable(tempPath);
            final CompletableFuture<Void> snapshotFuture = new CompletableFuture<>();
            final CompletableFuture<Void> sstFuture = createSstFiles(sstFileTable, region.getStartKey(),
                region.getEndKey(), executor);
            sstFuture.whenComplete((aVoid, throwable) -> {
                if (throwable == null) {
                    try {
                        final File snapshotFile = new File(snapshotPath);
                        FileUtils.deleteDirectory(snapshotFile);
                        if (!Utils.atomicMoveFile(tempFile, snapshotFile, true)) {
                            throw new StorageException("Fail to rename [" + tempPath + "] to [" + snapshotPath + "].");
                        }
                        snapshotFuture.complete(null);
                    } catch (final Throwable t) {
                        snapshotFuture.completeExceptionally(t);
                    }
                } else {
                    snapshotFuture.completeExceptionally(throwable);
                }
            });
            return snapshotFuture;
        } catch (final Exception e) {
            throw new StorageException("Fail to do read sst snapshot at path: " + snapshotPath, e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    void readSstSnapshot(final String snapshotPath) {
        final Timer.Context timeCtx = getTimeContext("READ_SST_SNAPSHOT");
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            final EnumMap<SstColumnFamily, File> sstFileTable = getSstFileTable(snapshotPath);
            ingestSstFiles(sstFileTable);
        } catch (final Exception e) {
            throw new StorageException("Fail to write sst snapshot at path: " + snapshotPath, e);
        } finally {
            readLock.unlock();
            timeCtx.stop();
        }
    }

    private EnumMap<SstColumnFamily, File> getSstFileTable(final String path) {
        final EnumMap<SstColumnFamily, File> sstFileTable = new EnumMap<>(SstColumnFamily.class);
        sstFileTable.put(SstColumnFamily.DEFAULT, Paths.get(path, "default.sst").toFile());
        sstFileTable.put(SstColumnFamily.SEQUENCE, Paths.get(path, "sequence.sst").toFile());
        sstFileTable.put(SstColumnFamily.LOCKING, Paths.get(path, "locking.sst").toFile());
        sstFileTable.put(SstColumnFamily.FENCING, Paths.get(path, "fencing.sst").toFile());
        return sstFileTable;
    }

    private ColumnFamilyHandle findColumnFamilyHandle(final SstColumnFamily sstColumnFamily) {
        switch (sstColumnFamily) {
            case DEFAULT:
                return this.defaultHandle;
            case SEQUENCE:
                return this.sequenceHandle;
            case LOCKING:
                return this.lockingHandle;
            case FENCING:
                return this.fencingHandle;
            default:
                throw new IllegalArgumentException("illegal sstColumnFamily: " + sstColumnFamily.name());
        }
    }

    private void openRocksDB(final RocksDBOptions opts) throws RocksDBException {
        final List<ColumnFamilyHandle> cfHandles = Lists.newArrayList();
        this.databaseVersion.incrementAndGet();
        this.db = RocksDB.open(this.options, opts.getDbPath(), this.cfDescriptors, cfHandles);
        this.defaultHandle = cfHandles.get(0);
        this.sequenceHandle = cfHandles.get(1);
        this.lockingHandle = cfHandles.get(2);
        this.fencingHandle = cfHandles.get(3);
    }

    private void closeRocksDB() {
        if (this.db != null) {
            this.db.close();
            this.db = null;
        }
    }

    private void destroyRocksDB(final RocksDBOptions opts) throws RocksDBException {
        // The major difference with directly deleting the DB directory manually is that
        // DestroyDB() will take care of the case where the RocksDB database is stored
        // in multiple directories. For instance, a single DB can be configured to store
        // its data in multiple directories by specifying different paths to
        // DBOptions::db_paths, DBOptions::db_log_dir, and DBOptions::wal_dir.
        try (final Options opt = new Options()) {
            RocksDB.destroyDB(opts.getDbPath(), opt);
        }
    }

    // Creates the rocksDB options, the user must take care
    // to close it after closing db.
    private static DBOptions createDBOptions() {
        return StorageOptionsFactory.getRocksDBOptions(RocksRawKVStore.class) //
            .setEnv(Env.getDefault());
    }

    // Creates the column family options to control the behavior
    // of a database.
    private static ColumnFamilyOptions createColumnFamilyOptions() {
        final BlockBasedTableConfig tConfig = StorageOptionsFactory.getRocksDBTableFormatConfig(RocksRawKVStore.class);
        return StorageOptionsFactory.getRocksDBColumnFamilyOptions(RocksRawKVStore.class) //
            .setTableFormatConfig(tConfig) //
            .setMergeOperator(new StringAppendOperator());
    }

    // Creates the backupable db options to control the behavior of
    // a backupable database.
    private static BackupableDBOptions createBackupDBOptions(final String backupDBPath) {
        return new BackupableDBOptions(backupDBPath) //
            .setSync(true) //
            .setShareTableFiles(false); // don't share data between backups
    }

    @Override
    public void describe(final Printer out) {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            if (this.db != null) {
                out.println(this.db.getProperty("rocksdb.stats"));
            }
            out.println("");
            if (this.statistics != null) {
                out.println(this.statistics.getString());
            }
        } catch (final RocksDBException e) {
            out.println(e);
        } finally {
            readLock.unlock();
        }
    }
}
