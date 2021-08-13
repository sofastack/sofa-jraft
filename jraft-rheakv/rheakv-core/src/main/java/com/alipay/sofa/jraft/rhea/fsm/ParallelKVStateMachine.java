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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.rhea.StateListener;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.fsm.dag.DagTaskGraph;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.DisruptorBasedPipeDecorator;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.KvPipe.KvTaskPipeline;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.KvPipe.RecyclableKvTask;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.KvPipe.RecyclableKvTask.TaskStatus;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.KvPipe.TaskDispatchPipe;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.ParallelSmrOptions;
import com.alipay.sofa.jraft.rhea.storage.BatchRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.KVStoreSnapshotFile;
import com.alipay.sofa.jraft.rhea.storage.KVStoreSnapshotFileFactory;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_APPLY_QPS;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_BATCH_WRITE;

/**
 * Parallel kv statemachine, use pipeline + disruptor + dagGraph
 * @author hzh (642256541@qq.com)
 */
public class ParallelKVStateMachine extends StateMachineAdapter implements Lifecycle<ParallelSmrOptions>, Runnable {
    private static final Logger                                             LOG        = LoggerFactory
                                                                                           .getLogger(ParallelKVStateMachine.class);

    private final AtomicLong                                                leaderTerm = new AtomicLong(-1L);
    private final Region                                                    region;
    private final StoreEngine                                               storeEngine;
    private final BatchRawKVStore<?>                                        rawKVStore;
    private final KVStoreSnapshotFile                                       storeSnapshotFile;
    private final Meter                                                     applyMeter;
    private final Histogram                                                 batchWriteHistogram;
    private final KvTaskPipeline                                            kvTaskPipeline;
    private final DagTaskGraph<RecyclableKvTask>                            dagTaskGraph;
    private final Thread                                                    graphConsumer;
    private volatile boolean                                                start      = false;
    private DisruptorBasedPipeDecorator<RecyclableKvTask, RecyclableKvTask> dispatchPipe;

    public ParallelKVStateMachine(Region region, StoreEngine storeEngine) {
        this.region = region;
        this.storeEngine = storeEngine;
        this.rawKVStore = storeEngine.getRawKVStore();
        this.storeSnapshotFile = KVStoreSnapshotFileFactory.getKVStoreSnapshotFile(this.rawKVStore);
        final String regionStr = String.valueOf(this.region.getId());
        this.applyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, regionStr);
        this.batchWriteHistogram = KVMetrics.histogram(STATE_MACHINE_BATCH_WRITE, regionStr);
        this.dagTaskGraph = new DagTaskGraph<>();
        this.kvTaskPipeline = new KvTaskPipeline(this.dagTaskGraph);
        this.graphConsumer = new Thread(this);
    }

    @Override
    public boolean init(final ParallelSmrOptions opts) {
        this.kvTaskPipeline.init(opts);
        this.dispatchPipe = new DisruptorBasedPipeDecorator<>(new TaskDispatchPipe(this, this.rawKVStore),
            opts.getDispatchPipeWorkerNums());
        this.dispatchPipe.init(kvTaskPipeline.getPipeContext());
        this.start = true;
        this.graphConsumer.start();
        System.out.println("start success");
        return true;
    }

    @Override
    public void shutdown() {
        this.start = false;
        this.graphConsumer.interrupt();
        this.kvTaskPipeline.shutdown();
        this.dispatchPipe.shutdown(1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onApply(final Iterator iter) {
        try {
            // Put iter to pipeline and wait to be scheduled
            this.kvTaskPipeline.process(iter);
        } catch (final InterruptedException e) {
            LOG.info("Parallel smr shutdown");
        }
    }

    @Override
    public void run() {
        // Take ready tasks from dag graph add send to dispatch pipe
        while (this.start) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            final List<RecyclableKvTask> readyTasks = this.dagTaskGraph.getReadyTasks();
            for (final RecyclableKvTask kvTask : readyTasks) {
                if (!kvTask.getTaskStatus().equals(TaskStatus.WAITING)) {
                    continue;
                }
                System.out.println("dispatch task:" + kvTask);
                this.dagTaskGraph.notifyStart(kvTask);
                kvTask.setTaskStatus(TaskStatus.RUNNING);
                kvTask.setDone((status) -> {
                    kvTask.setTaskStatus(TaskStatus.DONE);
                    this.dagTaskGraph.notifyDone(kvTask);
                    kvTask.recycle();
                });
                this.dispatchPipe.process(kvTask);
            }
        }
    }

    public void doSplit(final KVState kvState) {
        final byte[] parentKey = this.region.getStartKey();
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

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        this.storeSnapshotFile.save(writer, this.region.copy(), done, this.storeEngine.getSnapshotExecutor());
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot.");
            return false;
        }
        return this.storeSnapshotFile.load(reader, this.region.copy());
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.leaderTerm.set(term);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        final List<StateListener> listeners = this.storeEngine.getStateListenerContainer() //
                .getStateListenerGroup(getRegionId());
        if (listeners.isEmpty()) {
            return;
        }
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : listeners) { // iterator the snapshot
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
        final List<StateListener> listeners = this.storeEngine.getStateListenerContainer() //
                .getStateListenerGroup(getRegionId());
        if (listeners.isEmpty()) {
            return;
        }
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : listeners) { // iterator the snapshot
                listener.onLeaderStop(oldTerm);
            }
        });
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        super.onStartFollowing(ctx);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        final List<StateListener> listeners = this.storeEngine.getStateListenerContainer() //
                .getStateListenerGroup(getRegionId());
        if (listeners.isEmpty()) {
            return;
        }
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : listeners) { // iterator the snapshot
                listener.onStartFollowing(ctx.getLeaderId(), ctx.getTerm());
            }
        });
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        final List<StateListener> listeners = this.storeEngine.getStateListenerContainer() //
                .getStateListenerGroup(getRegionId());
        if (listeners.isEmpty()) {
            return;
        }
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : listeners) { // iterator the snapshot
                listener.onStopFollowing(ctx.getLeaderId(), ctx.getTerm());
            }
        });
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public long getRegionId() {
        return this.region.getId();
    }

    /**
     * Sets critical error and halt the state machine.
     *
     * If current node is a leader, first reply to client
     * failure response.
     *
     * @param closure callback
     * @param ex      critical error
     */
    private static void setCriticalError(final KVStoreClosure closure, final Throwable ex) {
        // Will call closure#run in FSMCaller
        if (closure != null) {
            closure.setError(Errors.forException(ex));
        }
        ThrowUtil.throwException(ex);
    }
}
