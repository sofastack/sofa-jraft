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

import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.FSMCaller.LastAppliedLogIndexListener;
import com.alipay.sofa.jraft.ReadOnlyService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.ReadIndexState;
import com.alipay.sofa.jraft.entity.ReadIndexStatus;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyServiceOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.util.Bytes;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.ZeroByteStringHelper;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * Read-only service implementation.
 * @author dennis
 *
 */
public class ReadOnlyServiceImpl implements ReadOnlyService, LastAppliedLogIndexListener {

    /** disruptor to run readonly service. */
    private Disruptor<ReadIndexEvent>                  readIndexDisruptor;
    private RingBuffer<ReadIndexEvent>                 readIndexQueue;
    private RaftOptions                                raftOptions;
    private NodeImpl                                   node;
    private final Lock                                 lock                = new ReentrantLock();
    private FSMCaller                                  fsmCaller;
    private volatile CountDownLatch                    shutdownLatch;

    private ScheduledExecutorService                   scheduledExecutorService;

    // <logIndex, statusList>
    private final TreeMap<Long, List<ReadIndexStatus>> pendingNotifyStatus = new TreeMap<>();

    private static class ReadIndexEvent {
        Bytes            requestContext;
        ReadIndexClosure done;
        CountDownLatch   shutdownLatch;
        long             startTime;
    }

    private static class ReadIndexEventFactory implements EventFactory<ReadIndexEvent> {

        @Override
        public ReadIndexEvent newInstance() {
            return new ReadIndexEvent();
        }
    }

    private class ReadIndexEventHandler implements EventHandler<ReadIndexEvent> {
        // task list for batch
        private List<ReadIndexEvent> events = new ArrayList<>(raftOptions.getApplyBatch());

        @Override
        public void onEvent(ReadIndexEvent newEvent, long sequence, boolean endOfBatch) throws Exception {
            if (newEvent.shutdownLatch != null) {
                executeReadIndexEvents(events);
                newEvent.shutdownLatch.countDown();
                return;
            }

            events.add(newEvent);
            if (events.size() >= raftOptions.getApplyBatch() || endOfBatch) {
                executeReadIndexEvents(events);
                events = new ArrayList<>(raftOptions.getApplyBatch());
            }
        }
    }

    /**
     * ReadIndexResponse process closure
     *
     * @author dennis
     */
    class ReadIndexResponseClosure extends RpcResponseClosureAdapter<ReadIndexResponse> {

        final List<ReadIndexEvent> events;
        final List<ReadIndexState> states;
        final ReadIndexRequest     request;

        public ReadIndexResponseClosure(List<ReadIndexEvent> events, List<ReadIndexState> states,
                                        final ReadIndexRequest request) {
            super();
            this.events = events;
            this.states = states;
            this.request = request;
        }

        /**
         * Called when ReadIndex response returns.
         */
        @Override
        public void run(Status status) {
            if (!status.isOk()) {
                notifyFail(status);
                return;
            }
            final ReadIndexResponse readIndexResponse = getResponse();
            if (!readIndexResponse.getSuccess()) {
                notifyFail(new Status(-1, "Fail to run ReadIndex task, maybe the leader stepped down."));
                return;
            }
            //Success
            final ReadIndexStatus readIndexStatus = new ReadIndexStatus(states, request, readIndexResponse.getIndex());
            for (final ReadIndexState state : states) {
                //Records current commit log index.
                state.setIndex(readIndexResponse.getIndex());
            }

            boolean doUnlock = true;
            lock.lock();
            try {
                if (readIndexStatus.isApplied(fsmCaller.getLastAppliedIndex())) {
                    // Already applied,notify readIndex request.
                    lock.unlock();
                    doUnlock = false;
                    notifySuccess(readIndexStatus);
                } else {
                    // Not applied, add it to pending-notify cache.
                    pendingNotifyStatus.computeIfAbsent(readIndexStatus.getIndex(), k -> new ArrayList<>(10))
                        .add(readIndexStatus);
                }
            } finally {
                if (doUnlock) {
                    lock.unlock();
                }
            }
        }

        private void notifyFail(Status status) {
            final long nowMs = Utils.monotonicMs();
            for (final ReadIndexEvent event : events) {
                node.getNodeMetrics().recordLatency("read-index", nowMs - event.startTime);
                if (event.done != null) {
                    event.done.run(status, ReadIndexClosure.INVALID_LOG_INDEX, event.requestContext.get());
                }
            }
        }

    }

    private void executeReadIndexEvents(List<ReadIndexEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        final ReadIndexRequest.Builder rb = ReadIndexRequest.newBuilder();
        rb.setGroupId(node.getGroupId());
        rb.setServerId(node.getServerId().toString());

        final List<ReadIndexState> states = new ArrayList<>(events.size());

        for (final ReadIndexEvent event : events) {
            rb.addEntries(ZeroByteStringHelper.wrap(event.requestContext.get()));
            states.add(new ReadIndexState(event.requestContext, event.done, event.startTime));
        }
        final ReadIndexRequest request = rb.build();

        node.handleReadIndexRequest(request, new ReadIndexResponseClosure(events, states, request));
    }

    @Override
    public boolean init(ReadOnlyServiceOptions opts) {
        this.node = opts.getNode();
        this.fsmCaller = opts.getFsmCaller();
        this.raftOptions = opts.getRaftOptions();

        this.scheduledExecutorService = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("ReadOnlyService-PendingNotify-Scanner", true));
        this.readIndexDisruptor = new Disruptor<>(new ReadIndexEventFactory(), raftOptions.getDisruptorBufferSize(),
                new NamedThreadFactory("JRaft-ReadOnlyService-Disruptor-", true));
        this.readIndexDisruptor.handleEventsWith(new ReadIndexEventHandler());
        this.readIndexDisruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(this.getClass().getSimpleName()));
        this.readIndexDisruptor.start();
        this.readIndexQueue = this.readIndexDisruptor.getRingBuffer();

        //listen on lastAppliedLogIndex change events.
        this.fsmCaller.addLastAppliedLogIndexListener(this);

        //start scanner
        this.scheduledExecutorService.scheduleAtFixedRate(() -> onApplied(this.fsmCaller.getLastAppliedIndex()),
                this.raftOptions.getMaxElectionDelayMs(), this.raftOptions.getMaxElectionDelayMs(), TimeUnit.MILLISECONDS);
        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.shutdownLatch != null) {
            return;
        }
        this.shutdownLatch = new CountDownLatch(1);
        this.readIndexQueue.publishEvent((event, sequence) -> event.shutdownLatch = this.shutdownLatch);
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            this.shutdownLatch.await();
        }
        this.readIndexDisruptor.shutdown();
        this.scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Override
    public void addRequest(byte[] reqCtx, ReadIndexClosure closure) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(closure, new Status(RaftError.EHOSTDOWN, "was stopped"));
            throw new IllegalStateException("Service already shutdown.");
        }
        try {
            this.readIndexQueue.publishEvent((event, sequence) -> {
                event.done = closure;
                event.requestContext = new Bytes(reqCtx);
                event.startTime = Utils.monotonicMs();
            });
        } catch (final Exception e) {
            Utils.runClosureInThread(closure, new Status(RaftError.EPERM, "Node is down."));
        }
    }

    /**
     * Called when lastAppliedIndex updates
     *
     * @param appliedIndex applied index
     */
    @Override
    public void onApplied(long appliedIndex) {
        // TODO reuse pendingStatuses list?
        List<ReadIndexStatus> pendingStatuses = null;
        lock.lock();
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
        } finally {
            lock.unlock();
            if (pendingStatuses != null && !pendingStatuses.isEmpty()) {
                for (final ReadIndexStatus status : pendingStatuses) {
                    notifySuccess(status);
                }
            }
        }
    }

    /**
     * Flush all events in disruptor.
     */
    @OnlyForTest
    void flush() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        this.readIndexQueue.publishEvent((task, sequence) -> task.shutdownLatch = latch);
        latch.await();
    }

    @OnlyForTest
    TreeMap<Long, List<ReadIndexStatus>> getPendingNotifyStatus() {
        return pendingNotifyStatus;
    }

    private void notifySuccess(final ReadIndexStatus status) {
        final long nowMs = Utils.monotonicMs();
        final List<ReadIndexState> states = status.getStates();
        final int taskCount = states.size();
        for (int i = 0; i < taskCount; i++) {
            final ReadIndexState task = states.get(i);
            final ReadIndexClosure done = task.getDone(); // stack copy
            if (done != null) {
                this.node.getNodeMetrics().recordLatency("read-index", nowMs - task.getStartTimeMs());
                done.setResult(task.getIndex(), task.getRequestContext().get());
                done.run(Status.OK());
            }
        }
    }

}
