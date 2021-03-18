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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.Clock;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;

/**
 * KVStore based on RAFT replica state machine.
 *
 * @author jiachun.fjc
 */
public class RaftRawKVStore implements RawKVStore {

    private static final Logger LOG = LoggerFactory.getLogger(RaftRawKVStore.class);

    private final Node          node;
    private final RawKVStore    kvStore;
    private final Executor      readIndexExecutor;

    public RaftRawKVStore(Node node, RawKVStore kvStore, Executor readIndexExecutor) {
        this.node = node;
        this.kvStore = kvStore;
        this.readIndexExecutor = readIndexExecutor;
    }

    @Override
    public KVIterator localIterator() {
        return this.kvStore.localIterator();
    }

    @Override
    public void get(final byte[] key, final KVStoreClosure closure) {
        get(key, true, closure);
    }

    @Override
    public void get(final byte[] key, final boolean readOnlySafe, final KVStoreClosure closure) {
        if (!readOnlySafe) {
            this.kvStore.get(key, false, closure);
            return;
        }
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.get(key, true, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [get] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createGet(key), closure);
                    } else {
                        LOG.warn("Fail to [get] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void multiGet(final List<byte[]> keys, final KVStoreClosure closure) {
        multiGet(keys, true, closure);
    }

    @Override
    public void multiGet(final List<byte[]> keys, final boolean readOnlySafe, final KVStoreClosure closure) {
        if (!readOnlySafe) {
            this.kvStore.multiGet(keys, false, closure);
            return;
        }
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.multiGet(keys, true, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [multiGet] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createMultiGet(keys), closure);
                    } else {
                        LOG.warn("Fail to [multiGet] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void containsKey(final byte[] key, final KVStoreClosure closure) {
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.containsKey(key, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [containsKey] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createContainsKey(key), closure);
                    } else {
                        LOG.warn("Fail to [containsKey] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
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
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final boolean returnValue, final KVStoreClosure closure) {
        if (!readOnlySafe) {
            this.kvStore.scan(startKey, endKey, limit, false, returnValue, closure);
            return;
        }
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.scan(startKey, endKey, limit, true, returnValue, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [scan] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createScan(startKey, endKey, limit, returnValue), closure);
                    } else {
                        LOG.warn("Fail to [scan] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
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
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                            final boolean returnValue, final KVStoreClosure closure) {
        if (!readOnlySafe) {
            this.kvStore.reverseScan(startKey, endKey, limit, false, returnValue, closure);
            return;
        }
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.reverseScan(startKey, endKey, limit, true, returnValue, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [reverseScan] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createReverseScan(startKey, endKey, limit, returnValue), closure);
                    } else {
                        LOG.warn("Fail to [reverseScan] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void getSequence(final byte[] seqKey, final int step, final KVStoreClosure closure) {
        if (step > 0) {
            applyOperation(KVOperation.createGetSequence(seqKey, step), closure);
            return;
        }
        // read-only (step==0)
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.getSequence(seqKey, 0, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [getSequence] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createGetSequence(seqKey, 0), closure);
                    } else {
                        LOG.warn("Fail to [getSequence] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void resetSequence(final byte[] seqKey, final KVStoreClosure closure) {
        applyOperation(KVOperation.createResetSequence(seqKey), closure);
    }

    @Override
    public void put(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createPut(key, value), closure);
    }

    @Override
    public void getAndPut(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createGetAndPut(key, value), closure);
    }

    @Override
    public void compareAndPut(final byte[] key, final byte[] expect, final byte[] update, final KVStoreClosure closure) {
        applyOperation(KVOperation.createCompareAndPut(key, expect, update), closure);
    }

    @Override
    public void merge(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createMerge(key, value), closure);
    }

    @Override
    public void put(final List<KVEntry> entries, final KVStoreClosure closure) {
        applyOperation(KVOperation.createPutList(entries), closure);
    }

    @Override
    public void compareAndPutAll(final List<CASEntry> entries, final KVStoreClosure closure) {
        applyOperation(KVOperation.createCompareAndPutAll(entries), closure);
    }

    @Override
    public void putIfAbsent(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createPutIfAbsent(key, value), closure);
    }

    @Override
    public void tryLockWith(final byte[] key, final byte[] fencingKey, final boolean keepLease,
                            final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        // The algorithm relies on the assumption that while there is no
        // synchronized clock across the processes, still the local time in
        // every process flows approximately at the same rate, with an error
        // which is small compared to the auto-release time of the lock.
        acquirer.setLockingTimestamp(Clock.defaultClock().getTime());
        applyOperation(KVOperation.createKeyLockRequest(key, fencingKey, Pair.of(keepLease, acquirer)), closure);
    }

    @Override
    public void releaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        applyOperation(KVOperation.createKeyLockReleaseRequest(key, acquirer), closure);
    }

    @Override
    public void delete(final byte[] key, final KVStoreClosure closure) {
        applyOperation(KVOperation.createDelete(key), closure);
    }

    @Override
    public void deleteRange(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        applyOperation(KVOperation.createDeleteRange(startKey, endKey), closure);
    }

    @Override
    public void delete(final List<byte[]> keys, final KVStoreClosure closure) {
        applyOperation(KVOperation.createDeleteList(keys), closure);
    }

    @Override
    public void execute(final NodeExecutor nodeExecutor, final boolean isLeader, final KVStoreClosure closure) {
        applyOperation(KVOperation.createNodeExecutor(nodeExecutor), closure);
    }

    private void applyOperation(final KVOperation op, final KVStoreClosure closure) {
        if (!isLeader()) {
            closure.setError(Errors.NOT_LEADER);
            closure.run(new Status(RaftError.EPERM, "Not leader"));
            return;
        }
        final Task task = new Task();
        task.setData(ByteBuffer.wrap(Serializers.getDefault().writeObject(op)));
        task.setDone(new KVClosureAdapter(closure, op));
        this.node.apply(task);
    }

    private boolean isLeader() {
        return this.node.isLeader(false);
    }
}
