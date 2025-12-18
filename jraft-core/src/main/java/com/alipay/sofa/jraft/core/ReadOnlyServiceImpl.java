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
package com.alipay.sofa.jraft.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alipay.sofa.jraft.util.ThreadPoolsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.FSMCaller.LastAppliedLogIndexListener;
import com.alipay.sofa.jraft.ReadOnlyService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.ReadIndexState;
import com.alipay.sofa.jraft.entity.ReadIndexStatus;
import com.alipay.sofa.jraft.error.OverloadException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyServiceOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.util.Bytes;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.EventBus;
import com.alipay.sofa.jraft.util.concurrent.EventBusFactory;
import com.alipay.sofa.jraft.util.concurrent.EventBusHandler;
import com.alipay.sofa.jraft.util.concurrent.EventBusOptions;
import com.google.protobuf.ZeroByteStringHelper;

/**
 * Read-only service implementation.
 *
 * @author dennis
 */
public class ReadOnlyServiceImpl implements ReadOnlyService, LastAppliedLogIndexListener {

    /** EventBus to run readonly service. */
    private EventBus<ReadIndexEvent>                   readIndexEventBus;
    private RaftOptions                                raftOptions;
    private NodeImpl                                   node;
    private final Lock                                 lock                = new ReentrantLock();
    private FSMCaller                                  fsmCaller;
    private volatile CountDownLatch                    shutdownLatch;

    private ScheduledExecutorService                   scheduledExecutorService;

    private NodeMetrics                                nodeMetrics;

    private volatile RaftException                     error;

    // <logIndex, statusList>
    private final TreeMap<Long, List<ReadIndexStatus>> pendingNotifyStatus = new TreeMap<>();

    private static final Logger                        LOG                 = LoggerFactory
                                                                               .getLogger(ReadOnlyServiceImpl.class);

    private static class ReadIndexEvent {
        ReadOnlyOption   readOnlyOptions;
        Bytes            requestContext;
        ReadIndexClosure done;
        CountDownLatch   shutdownLatch;
        long             startTime;
    }

    private class ReadIndexEventHandler implements EventBusHandler<ReadIndexEvent> {
        // task list for batch
        private final List<ReadIndexEvent> events = new ArrayList<>(
                                                      ReadOnlyServiceImpl.this.raftOptions.getApplyBatch());

        @Override
        public void onEvent(final ReadIndexEvent newEvent, final boolean endOfBatch) throws Exception {
            if (newEvent.shutdownLatch != null) {
                executeReadIndexEvents(this.events);
                this.events.clear();
                newEvent.shutdownLatch.countDown();
                return;
            }

            this.events.add(newEvent);
            if (this.events.size() >= ReadOnlyServiceImpl.this.raftOptions.getApplyBatch() || endOfBatch) {
                executeReadIndexEvents(this.events);
                this.events.clear();
            }
        }
    }

    /**
     * ReadIndexResponse process closure
     *
     * @author dennis
     */
    class ReadIndexResponseClosure extends RpcResponseClosureAdapter<ReadIndexResponse> {

        final List<ReadIndexState> states;
        final ReadIndexRequest     request;

        public ReadIndexResponseClosure(final List<ReadIndexState> states, final ReadIndexRequest request) {
            super();
            this.states = states;
            this.request = request;
        }

        /**
         * Called when ReadIndex response returns.
         */
        @Override
        public void run(final Status status) {
            if (!status.isOk()) {
                notifyFail(status);
                return;
            }
            final ReadIndexResponse readIndexResponse = getResponse();
            if (!readIndexResponse.getSuccess()) {
                notifyFail(new Status(-1, "Fail to run ReadIndex task, maybe the leader stepped down."));
                return;
            }
            // Success
            final ReadIndexStatus readIndexStatus = new ReadIndexStatus(this.states, this.request,
                readIndexResponse.getIndex());
            for (final ReadIndexState state : this.states) {
                // Records current commit log index.
                state.setIndex(readIndexResponse.getIndex());
            }

            boolean doUnlock = true;
            ReadOnlyServiceImpl.this.lock.lock();
            try {
                if (readIndexStatus.isApplied(ReadOnlyServiceImpl.this.fsmCaller.getLastAppliedIndex())) {
                    // Already applied, notify readIndex request.
                    ReadOnlyServiceImpl.this.lock.unlock();
                    doUnlock = false;
                    notifySuccess(readIndexStatus);
                } else {
                    if (readIndexStatus.isOverMaxReadIndexLag(ReadOnlyServiceImpl.this.fsmCaller.getLastAppliedIndex(), ReadOnlyServiceImpl.this.raftOptions.getMaxReadIndexLag())) {
                        ReadOnlyServiceImpl.this.lock.unlock();
                        doUnlock = false;
                        notifyFail(new Status(-1, "Fail to run ReadIndex task, the gap of current node's apply index between leader's commit index over maxReadIndexLag"));
                    } else  {
                        // Not applied, add it to pending-notify cache.
                        ReadOnlyServiceImpl.this.pendingNotifyStatus
                            .computeIfAbsent(readIndexStatus.getIndex(), k -> new ArrayList<>(10)) //
                            .add(readIndexStatus);
                    }
                }
            } finally {
                if (doUnlock) {
                    ReadOnlyServiceImpl.this.lock.unlock();
                }
            }
        }

        private void notifyFail(final Status status) {
            final long nowMs = Utils.monotonicMs();
            for (final ReadIndexState state : this.states) {
                ReadOnlyServiceImpl.this.nodeMetrics.recordLatency("read-index", nowMs - state.getStartTimeMs());
                final ReadIndexClosure done = state.getDone();
                if (done != null) {
                    final Bytes reqCtx = state.getRequestContext();
                    done.run(status, ReadIndexClosure.INVALID_LOG_INDEX, reqCtx != null ? reqCtx.get() : null);
                }
            }
            // Clear references to help GC, see https://github.com/sofastack/sofa-jraft/issues/1231
            this.states.clear();
        }
    }

    private void handleReadIndex(final ReadOnlyOption option, final List<ReadIndexEvent> events) {
        final ReadIndexRequest.Builder rb = ReadIndexRequest.newBuilder() //
                .setGroupId(this.node.getGroupId()) //
                .setServerId(this.node.getServerId().toString())
                .setReadOnlyOptions(ReadOnlyOption.convertMsgType(option));
        final List<ReadIndexState> states = events.stream()
                .filter(it -> option.equals(it.readOnlyOptions))
                .map(it -> {
             byte [] bytes = it.requestContext.get();
                    rb.addEntries(ZeroByteStringHelper.wrap(bytes == null? new byte[0]: bytes));
                    return new ReadIndexState(it.requestContext, it.done, it.startTime);
                })
                .collect(Collectors.toList());

        if (states.isEmpty()) {
            return;
        }
        final ReadIndexRequest request = rb.build();
        this.node.handleReadIndexRequest(request, new ReadIndexResponseClosure(states, request));
    }

    private void executeReadIndexEvents(final List<ReadIndexEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        handleReadIndex(ReadOnlyOption.ReadOnlySafe, events);
        handleReadIndex(ReadOnlyOption.ReadOnlyLeaseBased, events);
    }

    private void resetPendingStatusError(final Status st) {
        this.lock.lock();
        try {
            final Iterator<List<ReadIndexStatus>> it = this.pendingNotifyStatus.values().iterator();
            while (it.hasNext()) {
                final List<ReadIndexStatus> statuses = it.next();
                for (final ReadIndexStatus status : statuses) {
                    reportError(status, st);
                }
                it.remove();
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean init(final ReadOnlyServiceOptions opts) {
        this.node = opts.getNode();
        this.nodeMetrics = this.node.getNodeMetrics();
        this.fsmCaller = opts.getFsmCaller();
        this.raftOptions = opts.getRaftOptions();

        this.scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory("ReadOnlyService-PendingNotify-Scanner", true));

        final EventBusOptions eventBusOpts = new EventBusOptions()
            .setMode(this.raftOptions.getEventBusMode())
            .setName("JRaft-ReadOnlyService-EventBus")
            .setBufferSize(this.raftOptions.getDisruptorBufferSize())
            .setThreadFactory(new NamedThreadFactory("JRaft-ReadOnlyService-EventBus-", true));
        this.readIndexEventBus = EventBusFactory.create(eventBusOpts, new ReadIndexEventHandler());

        // listen on lastAppliedLogIndex change events.
        this.fsmCaller.addLastAppliedLogIndexListener(this);

        // start scanner
        this.scheduledExecutorService.scheduleAtFixedRate(() -> onApplied(this.fsmCaller.getLastAppliedIndex()),
            this.raftOptions.getMaxElectionDelayMs(), this.raftOptions.getMaxElectionDelayMs(), TimeUnit.MILLISECONDS);
        return true;
    }

    @Override
    public synchronized void setError(final RaftException error) {
        if (this.error == null) {
            this.error = error;
        }
    }

    @Override
    public synchronized void shutdown() {
        if (this.shutdownLatch != null) {
            return;
        }
        this.shutdownLatch = new CountDownLatch(1);
        ThreadPoolsFactory.runInThread(this.node.getGroupId(), () -> {
            final ReadIndexEvent event = new ReadIndexEvent();
            event.shutdownLatch = this.shutdownLatch;
            this.readIndexEventBus.publish(event);
        });
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            this.shutdownLatch.await();
        }
        resetPendingStatusError(new Status(RaftError.ESTOP, "Node is quit."));
        this.scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Override
    public void addRequest(byte[] reqCtx, ReadIndexClosure closure) {
        addRequest(this.node.getRaftOptions().getReadOnlyOptions(), reqCtx, closure);
    }

    @Override
    public void addRequest(final ReadOnlyOption readOnlyOptions, final byte[] reqCtx, final ReadIndexClosure closure) {
        if (this.shutdownLatch != null) {
            ThreadPoolsFactory.runClosureInThread(this.node.getGroupId(), closure, new Status(RaftError.EHOSTDOWN,
                "Was stopped"));
            throw new IllegalStateException("Service already shutdown.");
        }
        try {
            final ReadIndexEvent event = new ReadIndexEvent();
            event.readOnlyOptions = readOnlyOptions;
            event.done = closure;
            event.requestContext = new Bytes(reqCtx);
            event.startTime = Utils.monotonicMs();

            switch (this.node.getOptions().getApplyTaskMode()) {
                case Blocking:
                    this.readIndexEventBus.publish(event);
                    break;
                case NonBlocking:
                default:
                    if (!this.readIndexEventBus.tryPublish(event)) {
                        final String errorMsg = "Node is busy, has too many read-index requests, queue is full and bufferSize="
                                                + this.readIndexEventBus.getBufferSize();
                        ThreadPoolsFactory.runClosureInThread(this.node.getGroupId(), closure, new Status(
                            RaftError.EBUSY, errorMsg));
                        this.nodeMetrics.recordTimes("read-index-overload-times", 1);
                        LOG.warn("Node {} ReadOnlyServiceImpl readIndexEventBus is overload.", this.node.getNodeId());
                        if (closure == null) {
                            throw new OverloadException(errorMsg);
                        }
                    }
                    break;
            }
        } catch (final Exception e) {
            ThreadPoolsFactory.runClosureInThread(this.node.getGroupId(), closure, new Status(RaftError.EPERM,
                "Node is down."));
        }
    }

    /**
     * Called when lastAppliedIndex updates.
     *
     * @param appliedIndex applied index
     */
    @Override
    public void onApplied(final long appliedIndex) {
        // TODO reuse pendingStatuses list?
        List<ReadIndexStatus> pendingStatuses = null;
        this.lock.lock();
        try {
            if (this.pendingNotifyStatus.isEmpty()) {
                return;
            }
            // Find all statuses that log index less than or equal to appliedIndex.
            final Map<Long, List<ReadIndexStatus>> statuses = this.pendingNotifyStatus.headMap(appliedIndex, true);
            if (statuses != null) {
                pendingStatuses = new ArrayList<>(statuses.size() << 1);

                final Iterator<Map.Entry<Long, List<ReadIndexStatus>>> it = statuses.entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<Long, List<ReadIndexStatus>> entry = it.next();
                    pendingStatuses.addAll(entry.getValue());
                    // Remove the entry from statuses, it will also be removed in pendingNotifyStatus.
                    it.remove();
                }

            }

            /*
             * Remaining pending statuses are notified by error if it is presented.
             * When the node is in error state, consider following situations:
             * 1. If commitIndex > appliedIndex, then all pending statuses should be notified by error status.
             * 2. When commitIndex == appliedIndex, there will be no more pending statuses.
             */
            if (this.error != null) {
                resetPendingStatusError(this.error.getStatus());
            }
        } finally {
            this.lock.unlock();
            if (pendingStatuses != null && !pendingStatuses.isEmpty()) {
                for (final ReadIndexStatus status : pendingStatuses) {
                    notifySuccess(status);
                }
            }
        }
    }

    /**
     * Flush all events in event bus.
     */
    @OnlyForTest
    void flush() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final ReadIndexEvent event = new ReadIndexEvent();
        event.shutdownLatch = latch;
        this.readIndexEventBus.publish(event);
        latch.await();
    }

    @OnlyForTest
    TreeMap<Long, List<ReadIndexStatus>> getPendingNotifyStatus() {
        return this.pendingNotifyStatus;
    }

    @OnlyForTest
    RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    private void reportError(final ReadIndexStatus status, final Status st) {
        final long nowMs = Utils.monotonicMs();
        final List<ReadIndexState> states = status.getStates();
        final int taskCount = states.size();
        for (int i = 0; i < taskCount; i++) {
            final ReadIndexState task = states.get(i);
            final ReadIndexClosure done = task.getDone();
            if (done != null) {
                this.nodeMetrics.recordLatency("read-index", nowMs - task.getStartTimeMs());
                done.run(st);
            }
        }
        // Clear references to help GC, see https://github.com/sofastack/sofa-jraft/issues/1231
        states.clear();
    }

    private void notifySuccess(final ReadIndexStatus status) {
        final long nowMs = Utils.monotonicMs();
        final List<ReadIndexState> states = status.getStates();
        final int taskCount = states.size();
        for (int i = 0; i < taskCount; i++) {
            final ReadIndexState task = states.get(i);
            final ReadIndexClosure done = task.getDone(); // stack copy
            if (done != null) {
                this.nodeMetrics.recordLatency("read-index", nowMs - task.getStartTimeMs());
                done.setResult(task.getIndex(), task.getRequestContext().get());
                done.run(Status.OK());
            }
        }
        // Clear references to help GC, see https://github.com/sofastack/sofa-jraft/issues/1231
        states.clear();
    }
}
