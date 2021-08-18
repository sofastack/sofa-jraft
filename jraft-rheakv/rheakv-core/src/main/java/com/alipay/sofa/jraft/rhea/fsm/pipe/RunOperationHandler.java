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
package com.alipay.sofa.jraft.rhea.fsm.pipe;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.IllegalKVOperationException;
import com.alipay.sofa.jraft.rhea.fsm.ParallelKVStateMachine;
import com.alipay.sofa.jraft.rhea.storage.BaseRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.lmax.disruptor.WorkHandler;

import java.util.List;

/**
 * @author hzh (642256541@qq.com)
 */
public class RunOperationHandler implements WorkHandler<KvEvent> {

    private final BaseRawKVStore<?>      rawKVStore;
    private final ParallelKVStateMachine parallelKVStateMachine;

    public RunOperationHandler(final ParallelKVStateMachine stateMachine, final BaseRawKVStore<?> rawKVStore) {
        this.parallelKVStateMachine = stateMachine;
        this.rawKVStore = rawKVStore;
    }

    @Override
    public void onEvent(final KvEvent event) {
        final RecyclableKvTask task = event.getTask();
        final List<KVState> kvStateList = task.getKvStateList();
        final Closure done = task.getDone();
        for (final KVState kvState : kvStateList) {
            final KVOperation op = kvState.getOp();
            //System.out.println("dispatch op: "+  op );
            switch (kvState.getOp().getOp()) {
                case KVOperation.PUT:
                    this.rawKVStore.put(op.getKey(), op.getValue(), kvState.getDone());
                    break;
                case KVOperation.PUT_IF_ABSENT:
                    this.rawKVStore.putIfAbsent(op.getKey(), op.getValue(), kvState.getDone());
                    break;
                case KVOperation.PUT_LIST:
                    this.rawKVStore.put(kvState.getOp().getEntries(), kvState.getDone());
                    break;
                case KVOperation.DELETE:
                    this.rawKVStore.delete(kvState.getOp().getKey(), kvState.getDone());
                    break;
                case KVOperation.DELETE_RANGE:
                    this.rawKVStore.deleteRange(op.getStartKey(), op.getEndKey(), kvState.getDone());
                    break;
                case KVOperation.DELETE_LIST:
                    this.rawKVStore.delete(kvState.getOp().getKeys(), kvState.getDone());
                    break;
                case KVOperation.GET_SEQUENCE:
                    this.rawKVStore.getSequence(op.getSeqKey(), op.getStep(), kvState.getDone());
                    break;
                case KVOperation.NODE_EXECUTE:
                    this.rawKVStore.execute(kvState.getOp().getNodeExecutor(), this.parallelKVStateMachine.isLeader(),
                        kvState.getDone());
                    break;
                case KVOperation.KEY_LOCK:
                    final Pair<Boolean, DistributedLock.Acquirer> acquirerPair = op.getAcquirerPair();
                    this.rawKVStore.tryLockWith(op.getKey(), op.getFencingKey(), acquirerPair.getKey(),
                        acquirerPair.getValue(), kvState.getDone());
                    break;
                case KVOperation.KEY_LOCK_RELEASE:
                    this.rawKVStore.releaseLockWith(op.getKey(), op.getAcquirer(), kvState.getDone());
                    break;
                case KVOperation.GET:
                    this.rawKVStore.get(kvState.getOp().getKey(), kvState.getDone());
                    break;
                case KVOperation.MULTI_GET:
                    this.rawKVStore.multiGet(kvState.getOp().getKeyList(), kvState.getDone());
                    break;
                case KVOperation.CONTAINS_KEY:
                    this.rawKVStore.containsKey(kvState.getOp().getKey(), kvState.getDone());
                    break;
                case KVOperation.SCAN:
                    this.rawKVStore.scan(op.getStartKey(), op.getEndKey(), op.getLimit(), true, op.isReturnValue(),
                        kvState.getDone());
                    break;
                case KVOperation.REVERSE_SCAN:
                    this.rawKVStore.reverseScan(op.getStartKey(), op.getEndKey(), op.getLimit(), true,
                        op.isReturnValue(), kvState.getDone());
                    break;
                case KVOperation.GET_PUT:
                    this.rawKVStore.getAndPut(op.getKey(), op.getValue(), kvState.getDone());
                    break;
                case KVOperation.COMPARE_PUT:
                    this.rawKVStore.compareAndPut(op.getKey(), op.getExpect(), op.getValue(), kvState.getDone());
                    break;
                case KVOperation.COMPARE_PUT_ALL:
                    this.rawKVStore.compareAndPutAll(kvState.getOp().getCASEntries(), kvState.getDone());
                    break;
                case KVOperation.MERGE:
                    this.rawKVStore.merge(op.getKey(), op.getValue(), kvState.getDone());
                    break;
                case KVOperation.RESET_SEQUENCE:
                    this.rawKVStore.resetSequence(kvState.getOp().getKey(), kvState.getDone());
                    break;
                case KVOperation.RANGE_SPLIT:
                    this.parallelKVStateMachine.doSplit(kvState);
                    break;
                default:
                    throw new IllegalKVOperationException("Unknown operation: " + kvState.getOp().getOp());
            }
        }
        if (done != null) {
            done.run(Status.OK());
        }
    }
}
