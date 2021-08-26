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
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.rhea.StateListener;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.BatchRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.KVStoreSnapshotFile;
import com.alipay.sofa.jraft.rhea.storage.KVStoreSnapshotFileFactory;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_APPLY_QPS;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_BATCH_WRITE;

/**
 * Base kv state machine
 *
 * @author jiachun.fjc
 */
public abstract class BaseKVStateMachine extends StateMachineAdapter {
    private static final Logger         LOG        = LoggerFactory.getLogger(KVStoreStateMachine.class);

    protected final AtomicLong          leaderTerm = new AtomicLong(-1L);
    protected final Serializer          serializer = Serializers.getDefault();
    protected final Region              region;
    protected final StoreEngine         storeEngine;
    protected final BatchRawKVStore<?>  rawKVStore;
    protected final KVStoreSnapshotFile storeSnapshotFile;
    protected final Meter               applyMeter;
    protected final Histogram           batchWriteHistogram;

    public BaseKVStateMachine(Region region, StoreEngine storeEngine) {
        this.region = region;
        this.storeEngine = storeEngine;
        this.rawKVStore = storeEngine.getRawKVStore();
        this.storeSnapshotFile = KVStoreSnapshotFileFactory.getKVStoreSnapshotFile(this.rawKVStore);
        final String regionStr = String.valueOf(this.region.getId());
        this.applyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, regionStr);
        this.batchWriteHistogram = KVMetrics.histogram(STATE_MACHINE_BATCH_WRITE, regionStr);
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
    protected static void setCriticalError(final KVStoreClosure closure, final Throwable ex) {
        // Will call closure#run in FSMCaller
        if (closure != null) {
            closure.setError(Errors.forException(ex));
        }
        ThrowUtil.throwException(ex);
    }
}
