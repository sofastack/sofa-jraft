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
package com.alipay.sofa.jraft.rhea.fsm;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.errors.IllegalKVOperationException;
import com.alipay.sofa.jraft.rhea.errors.StoreCodecException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.storage.KVClosureAdapter;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.storage.KVStateOutputList;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.RecycleUtil;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_APPLY_QPS;

/**
 * Rhea KV store state machine
 * Default kv state machine
 * @author jiachun.fjc
 */
public class KVStoreStateMachine extends BaseKVStateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(KVStoreStateMachine.class);

    public KVStoreStateMachine(Region region, StoreEngine storeEngine) {
        super(region, storeEngine);
    }

    @Override
    public void onApply(final Iterator it) {
        int index = 0;
        int applied = 0;
        try {
            KVStateOutputList kvStates = KVStateOutputList.newInstance();
            while (it.hasNext()) {
                KVOperation kvOp;
                final KVClosureAdapter done = (KVClosureAdapter) it.done();
                if (done != null) {
                    kvOp = done.getOperation();
                } else {
                    final ByteBuffer buf = it.getData();
                    try {
                        if (buf.hasArray()) {
                            kvOp = this.serializer.readObject(buf.array(), KVOperation.class);
                        } else {
                            kvOp = this.serializer.readObject(buf, KVOperation.class);
                        }
                    } catch (final Throwable t) {
                        ++index;
                        throw new StoreCodecException("Decode operation error", t);
                    }
                }
                final KVState first = kvStates.getFirstElement();
                if (first != null && !first.isSameOp(kvOp)) {
                    applied += batchApplyAndRecycle(first.getOpByte(), kvStates);
                    kvStates = KVStateOutputList.newInstance();
                }
                kvStates.add(KVState.of(kvOp, done));
                ++index;
                it.next();
            }
            if (!kvStates.isEmpty()) {
                final KVState first = kvStates.getFirstElement();
                assert first != null;
                applied += batchApplyAndRecycle(first.getOpByte(), kvStates);
            }
        } catch (final Throwable t) {
            LOG.error("StateMachine meet critical error: {}.", StackTraceUtil.stackTrace(t));
            it.setErrorAndRollback(index - applied, new Status(RaftError.ESTATEMACHINE,
                "StateMachine meet critical error: %s.", t.getMessage()));
        } finally {
            // metrics: qps
            this.applyMeter.mark(applied);
        }
    }

    private int batchApplyAndRecycle(final byte opByte, final KVStateOutputList kvStates) {
        try {
            final int size = kvStates.size();

            if (size == 0) {
                return 0;
            }

            if (!KVOperation.isValidOp(opByte)) {
                throw new IllegalKVOperationException("Unknown operation: " + opByte);
            }

            // metrics: op qps
            final Meter opApplyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, String.valueOf(this.region.getId()),
                KVOperation.opName(opByte));
            opApplyMeter.mark(size);
            this.batchWriteHistogram.update(size);

            // do batch apply
            batchApply(opByte, kvStates);

            return size;
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
            case KVOperation.DELETE_LIST:
                this.rawKVStore.batchDeleteList(kvStates);
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
            case KVOperation.CONTAINS_KEY:
                this.rawKVStore.batchContainsKey(kvStates);
                break;
            case KVOperation.SCAN:
                this.rawKVStore.batchScan(kvStates);
                break;
            case KVOperation.REVERSE_SCAN:
                this.rawKVStore.batchReverseScan(kvStates);
                break;
            case KVOperation.GET_PUT:
                this.rawKVStore.batchGetAndPut(kvStates);
                break;
            case KVOperation.COMPARE_PUT:
                this.rawKVStore.batchCompareAndPut(kvStates);
                break;
            case KVOperation.COMPARE_PUT_ALL:
                this.rawKVStore.batchCompareAndPutAll(kvStates);
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
            final long currentRegionId = op.getCurrentRegionId();
            final long newRegionId = op.getNewRegionId();
            final byte[] splitKey = op.getKey();
            final KVStoreClosure closure = kvState.getDone();
            try {
                this.rawKVStore.initFencingToken(parentKey, splitKey);
                this.storeEngine.doSplit(currentRegionId, newRegionId, splitKey);
                if (closure != null) {
                    // null on follower
                    closure.setData(Boolean.TRUE);
                    closure.run(Status.OK());
                }
            } catch (final Throwable t) {
                LOG.error("Fail to split, regionId={}, newRegionId={}, splitKey={}.", currentRegionId, newRegionId,
                    BytesUtil.toHex(splitKey));
                setCriticalError(closure, t);
            }
        }
    }

}
