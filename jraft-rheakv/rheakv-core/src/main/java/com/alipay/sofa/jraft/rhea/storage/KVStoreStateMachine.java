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

import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.IllegalKVOperationException;
import com.alipay.sofa.jraft.rhea.errors.StoreCodecException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.RecycleUtil;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.ZipUtil;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import static com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_APPLY_QPS;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_BATCH_WRITE;

/**
 * Rhea KV store state machine
 *
 * @author jiachun.fjc
 */
public class KVStoreStateMachine extends StateMachineAdapter {

    private static final Logger             LOG              = LoggerFactory.getLogger(KVStoreStateMachine.class);

    private static final String             SNAPSHOT_DIR     = "kv";
    private static final String             SNAPSHOT_ARCHIVE = "kv.zip";

    private final List<LeaderStateListener> listeners        = new CopyOnWriteArrayList<>();
    private final AtomicLong                leaderTerm       = new AtomicLong(-1L);
    private final Serializer                serializer       = Serializers.getDefault();
    private final Region                    region;
    private final StoreEngine               storeEngine;
    private final BatchRawKVStore<?>        rawKVStore;
    private final Meter                     applyMeter;
    private final Histogram                 batchWriteHistogram;

    public KVStoreStateMachine(Region region, StoreEngine storeEngine) {
        this.region = region;
        this.storeEngine = storeEngine;
        this.rawKVStore = storeEngine.getRawKVStore();
        final String regionStr = String.valueOf(this.region.getId());
        this.applyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, regionStr);
        this.batchWriteHistogram = KVMetrics.histogram(STATE_MACHINE_BATCH_WRITE, regionStr);
    }

    @Override
    public void onApply(final Iterator it) {
        int stCount = 0;
        KVStateOutputList kvStates = KVStateOutputList.newInstance();
        while (it.hasNext()) {
            KVOperation kvOp;
            final KVClosureAdapter done = (KVClosureAdapter) it.done();
            if (done != null) {
                kvOp = done.getOperation();
            } else {
                final byte[] data = it.getData().array();
                try {
                    kvOp = this.serializer.readObject(data, KVOperation.class);
                } catch (final Throwable t) {
                    throw new StoreCodecException("Decode operation error", t);
                }
            }
            final KVState first = kvStates.getFirstElement();
            if (first != null && !first.isSameOp(kvOp)) {
                batchApplyAndRecycle(first.getOpByte(), kvStates);
                kvStates = KVStateOutputList.newInstance();
            }
            kvStates.add(KVState.of(kvOp, done));
            ++stCount;
            it.next();
        }
        if (!kvStates.isEmpty()) {
            final KVState first = kvStates.getFirstElement();
            assert first != null;
            batchApplyAndRecycle(first.getOpByte(), kvStates);
        }

        // metrics: qps
        this.applyMeter.mark(stCount);
    }

    private void batchApplyAndRecycle(final byte opByte, final KVStateOutputList kvStates) {
        try {
            if (kvStates.isEmpty()) {
                return;
            }
            if (!KVOperation.isValidOp(opByte)) {
                throw new IllegalKVOperationException("Unknown operation: " + opByte);
            }

            // metrics: op qps
            final Meter opApplyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, String.valueOf(this.region.getId()),
                KVOperation.opName(opByte));
            final int size = kvStates.size();
            opApplyMeter.mark(size);
            this.batchWriteHistogram.update(size);

            // do batch apply
            batchApply(opByte, kvStates);
        } finally {
            RecycleUtil.recycle(kvStates);
        }
    }

    private void batchApply(final byte opType, final KVStateOutputList kvStates) {
        switch (opType) {
            case KVOperation.PUT:
                this.rawKVStore.batchPut(kvStates);
                break;
            case KVOperation.PUT_IF_ABSENT:
                this.rawKVStore.batchPutIfAbsent(kvStates);
                break;
            case KVOperation.PUT_LIST:
                this.rawKVStore.batchPutList(kvStates);
                break;
            case KVOperation.DELETE:
                this.rawKVStore.batchDelete(kvStates);
                break;
            case KVOperation.DELETE_RANGE:
                this.rawKVStore.batchDeleteRange(kvStates);
                break;
            case KVOperation.GET_SEQUENCE:
                this.rawKVStore.batchGetSequence(kvStates);
                break;
            case KVOperation.NODE_EXECUTE:
                this.rawKVStore.batchNodeExecute(kvStates, isLeader());
                break;
            case KVOperation.KEY_LOCK:
                this.rawKVStore.batchTryLockWith(kvStates);
                break;
            case KVOperation.KEY_LOCK_RELEASE:
                this.rawKVStore.batchReleaseLockWith(kvStates);
                break;
            case KVOperation.GET:
                this.rawKVStore.batchGet(kvStates);
                break;
            case KVOperation.MULTI_GET:
                this.rawKVStore.batchMultiGet(kvStates);
                break;
            case KVOperation.SCAN:
                this.rawKVStore.batchScan(kvStates);
                break;
            case KVOperation.GET_PUT:
                this.rawKVStore.batchGetAndPut(kvStates);
                break;
            case KVOperation.MERGE:
                this.rawKVStore.batchMerge(kvStates);
                break;
            case KVOperation.RESET_SEQUENCE:
                this.rawKVStore.batchResetSequence(kvStates);
                break;
            case KVOperation.RANGE_SPLIT:
                doSplit(kvStates);
                break;
            default:
                throw new IllegalKVOperationException("Unknown operation: " + opType);
        }
    }

    private void doSplit(final KVStateOutputList kvStates) {
        final byte[] parentKey = this.region.getStartKey();
        for (final KVState kvState : kvStates) {
            final KVOperation op = kvState.getOp();
            final Pair<Long, Long> regionIds = op.getRegionIds();
            final byte[] splitKey = op.getKey();
            final KVStoreClosure closure = kvState.getDone();
            try {
                this.rawKVStore.initFencingToken(parentKey, splitKey);
                this.storeEngine.doSplit(regionIds.getKey(), regionIds.getValue(), splitKey, closure);
            } catch (final Exception e) {
                if (closure != null) {
                    // closure is null on follower node
                    closure.setError(Errors.STORAGE_ERROR);
                    closure.run(new Status(RaftError.EIO, e.getMessage()));
                }
            }
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        final String snapshotPath = Paths.get(writer.getPath(), SNAPSHOT_DIR).toString();
        try {
            final LocalFileMeta meta = this.rawKVStore.onSnapshotSave(snapshotPath, this.region.copy());
            this.storeEngine.getSnapshotExecutor().execute(() -> doCompressSnapshot(writer, meta, done));
        } catch (final Throwable t) {
            LOG.error("Fail to save snapshot at {}, {}.", snapshotPath, StackTraceUtil.stackTrace(t));
            done.run(new Status(RaftError.EIO, "Fail to save snapshot at %s, error is %s", snapshotPath,
                    t.getMessage()));
        }
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot.");
            return false;
        }
        final LocalFileMeta meta = (LocalFileMeta) reader.getFileMeta(SNAPSHOT_ARCHIVE);
        if (meta == null) {
            LOG.error("Can't find kv snapshot file at {}.", reader.getPath());
            return false;
        }
        final String sourceFile = Paths.get(reader.getPath(), SNAPSHOT_ARCHIVE).toString();
        final String snapshotPath = Paths.get(reader.getPath(), SNAPSHOT_DIR).toString();
        try {
            ZipUtil.unzipFile(sourceFile, reader.getPath());
            this.rawKVStore.onSnapshotLoad(snapshotPath, meta, this.region.copy());
            return true;
        } catch (final Throwable t) {
            LOG.error("Fail to load snapshot: {}.", StackTraceUtil.stackTrace(t));
            return false;
        }
    }

    private void doCompressSnapshot(final SnapshotWriter writer, final LocalFileMeta meta, final Closure done) {
        final String backupPath = Paths.get(writer.getPath(), SNAPSHOT_DIR).toString();
        final String outputFile = Paths.get(writer.getPath(), SNAPSHOT_ARCHIVE).toString();
        try {
            try (final ZipOutputStream out = new ZipOutputStream(new FileOutputStream(outputFile))) {
                ZipUtil.compressDirectoryToZipFile(writer.getPath(), SNAPSHOT_DIR, out);
            }
            if (writer.addFile(SNAPSHOT_ARCHIVE, meta)) {
                done.run(Status.OK());
            } else {
                done.run(new Status(RaftError.EIO, "Fail to add snapshot file: %s", backupPath));
            }
        } catch (final Throwable t) {
            LOG.error("Fail to save snapshot at {}, {}.", backupPath, StackTraceUtil.stackTrace(t));
            done.run(new Status(RaftError.EIO, "Fail to save snapshot at %s, error is %s", backupPath, t.getMessage()));
        }
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.leaderTerm.set(term);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        this.storeEngine.getLeaderStateTrigger().execute(() -> {
            for (final LeaderStateListener listener : this.listeners) { // iterator the snapshot
                listener.onLeaderStart(term);
            }
        });
    }

    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        final long oldTerm = this.leaderTerm.get();
        this.leaderTerm.set(-1L);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we asynchronously
        // triggers the listeners.
        this.storeEngine.getLeaderStateTrigger().execute(() -> {
            for (final LeaderStateListener listener : this.listeners) { // iterator the snapshot
                listener.onLeaderStop(oldTerm);
            }
        });
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public void addLeaderStateListener(final LeaderStateListener listener) {
        this.listeners.add(listener);
    }

    public long getRegionId() {
        return this.region.getId();
    }
}
