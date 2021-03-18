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

import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;

/**
 * The default batch write implementation, without any optimization,
 * subclasses need to override and optimize.
 *
 * @author jiachun.fjc
 */
public abstract class BatchRawKVStore<T> extends BaseRawKVStore<T> {

    public void batchPut(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            put(op.getKey(), op.getValue(), kvState.getDone());
        }
    }

    public void batchPutIfAbsent(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            putIfAbsent(op.getKey(), op.getValue(), kvState.getDone());
        }
    }

    public void batchPutList(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            put(kvState.getOp().getEntries(), kvState.getDone());
        }
    }

    public void batchDelete(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            delete(kvState.getOp().getKey(), kvState.getDone());
        }
    }

    public void batchDeleteRange(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            deleteRange(op.getStartKey(), op.getEndKey(), kvState.getDone());
        }
    }

    public void batchDeleteList(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            delete(kvState.getOp().getKeys(), kvState.getDone());
        }
    }

    public void batchGetSequence(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            getSequence(op.getSeqKey(), op.getStep(), kvState.getDone());
        }
    }

    public void batchNodeExecute(final KVStateOutputList kvStates, final boolean isLeader) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            execute(kvState.getOp().getNodeExecutor(), isLeader, kvState.getDone());
        }
    }

    public void batchTryLockWith(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            final Pair<Boolean, DistributedLock.Acquirer> acquirerPair = op.getAcquirerPair();
            tryLockWith(op.getKey(), op.getFencingKey(), acquirerPair.getKey(), acquirerPair.getValue(),
                kvState.getDone());
        }
    }

    public void batchReleaseLockWith(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            releaseLockWith(op.getKey(), op.getAcquirer(), kvState.getDone());
        }
    }

    public void batchGet(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            get(kvState.getOp().getKey(), kvState.getDone());
        }
    }

    public void batchMultiGet(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            multiGet(kvState.getOp().getKeyList(), kvState.getDone());
        }
    }

    public void batchContainsKey(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            containsKey(kvState.getOp().getKey(), kvState.getDone());
        }
    }

    public void batchScan(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            scan(op.getStartKey(), op.getEndKey(), op.getLimit(), true, op.isReturnValue(), kvState.getDone());
        }
    }

    public void batchReverseScan(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            reverseScan(op.getStartKey(), op.getEndKey(), op.getLimit(), true, op.isReturnValue(), kvState.getDone());
        }
    }

    public void batchGetAndPut(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            getAndPut(op.getKey(), op.getValue(), kvState.getDone());
        }
    }

    public void batchCompareAndPut(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            compareAndPut(op.getKey(), op.getExpect(), op.getValue(), kvState.getDone());
        }
    }

    public void batchCompareAndPutAll(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            compareAndPutAll(kvState.getOp().getCASEntries(), kvState.getDone());
        }
    }

    public void batchMerge(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            merge(op.getKey(), op.getValue(), kvState.getDone());
        }
    }

    public void batchResetSequence(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            resetSequence(kvState.getOp().getKey(), kvState.getDone());
        }
    }
}
