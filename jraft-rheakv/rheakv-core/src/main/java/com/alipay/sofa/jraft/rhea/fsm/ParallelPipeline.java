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
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.fsm.dag.DagGraph;
import com.alipay.sofa.jraft.rhea.fsm.dag.DefaultDagGraph;
import com.alipay.sofa.jraft.rhea.fsm.dag.GraphNode;
import com.alipay.sofa.jraft.rhea.fsm.pipe.DetectDependencyHandler;
import com.alipay.sofa.jraft.rhea.fsm.pipe.ParserKeyHandler;
import com.alipay.sofa.jraft.rhea.fsm.pipe.RecyclableKvTask;
import com.alipay.sofa.jraft.rhea.fsm.pipe.RunCommandHandler;
import com.alipay.sofa.jraft.rhea.options.ParallelSmrOptions;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.BaseRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVClosureAdapter;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.util.DisruptorBuilder;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Pipeline of parallel kv statemachine, base on disruptor + dagGraph:
 * ---------------------------------------------------------------------------------------------------------------------------
 *      StateMachine             PreProcessDisruptor               DagGraph                          RunCommandDisruptor
 *      │                        │                                 │                                 │
 *      │                        │                                 │                                 │
 *      ▼                       ▼                                ▼                                 ▼
 *     ┌─────────────────────────┬─────────────────────────────────┬─────────────────────────────────┬─────────────────────────┐
 *
 *      DispatchBatch ---------->  ParKey ---> DetectDependencies     -----> DagGraph --->  Schedule  ---> ReadyTask : RunCommand
 *
 *     └─────────────────────────┴─────────────────────────────────┴─────────────────────────────────┴─────────────────────────┘
 * ---------------------------------------------------------------------------------------------------------------------------
 *
 * 1.First stage(preProcessDisruptor):
 * ParseKeyHandler : Parse key from kvStateList, detect specific operations
 * ->
 * DetectDependencyHandler:  Detect dependencies between current task and all pre tasks
 *
 * 2.Second stage(dagGraph):
 * add to dagGraph, wait to be scheduled
 *
 * 3.Third stage(timer + runCommandDisruptor):
 * timer schedules ready tasks to runCommandDisruptor
 */
public class ParallelPipeline implements Lifecycle<ParallelSmrOptions> {

    private static final Logger                 LOG  = LoggerFactory.getLogger(ParallelPipeline.class);

    private final DagGraph<RecyclableKvTask>    dagGraph;
    private final BaseRawKVStore<?>             rawKVStore;
    private final ParallelKVStateMachine        stateMachine;
    private final Serializer                    serializer   = Serializers.getDefault();

    private ParallelSmrOptions                  smrOptions;
    private Disruptor<KvEvent>                  preProcessDisruptor;
    private Disruptor<KvEvent>                  runCommandDisruptor;

    private final EventFactory<KvEvent>         eventFactory = KvEvent::new;
    private final ScheduledExecutorService      scheduler = Executors.newSingleThreadScheduledExecutor();


    /**
     *
     * Wrapper of RecyclableKvTask
     */
    public static class KvEvent {

        private RecyclableKvTask task = null;

        public void setTask(final RecyclableKvTask task) {
            this.task = task;
        }

        public RecyclableKvTask getTask() {
            return task;
        }

        public void reset() {
            this.task = null;
        }

    }


    private class ReadyTaskScheduler implements Runnable {

        final DagGraph<RecyclableKvTask>  dagGraph;

        public ReadyTaskScheduler(final DagGraph<RecyclableKvTask> dagGraph) {
            this.dagGraph = dagGraph;
        }

        @Override
        public void run() {
            // Get ready tasks from dag graph, and send to runCommandDisruptor
            final List<GraphNode<RecyclableKvTask>> readyTasks = this.dagGraph.getReadyNodes();
            for (final GraphNode<RecyclableKvTask> node : readyTasks) {
                if (!node.isWaiting()) {
                    continue;
                }
                final RecyclableKvTask kvTask = node.getItem();
                kvTask.setDone((status) -> {
                    this.dagGraph.notifyDone(node);
                    kvTask.recycle();
                });
                this.dagGraph.notifyStart(node);
                dispatchReadyTask(kvTask);
            }
        }
    }


    public ParallelPipeline(final ParallelKVStateMachine stateMachine, final BaseRawKVStore<?> rawKVStore) {
        this.dagGraph = new DefaultDagGraph<>();
        this.stateMachine = stateMachine;
        this.rawKVStore = rawKVStore;
    }


    @Override
    public boolean init(final ParallelSmrOptions opts) {
        this.smrOptions = opts;
        final int bufferSize = opts.getPipelineBufferSize();

        this.preProcessDisruptor = buildDisruptor(bufferSize);
        this.runCommandDisruptor = buildDisruptor(bufferSize);

        // Init Pipeline
        final WorkHandler<KvEvent>[] parseKeyHandlers = buildWorkHandlers(opts.getParserWorker(), ParserKeyHandler.class);
        final WorkHandler<KvEvent>[] detectHandlers = buildWorkHandlers(opts.getDetectorWorker(), DetectDependencyHandler.class);
        final WorkHandler<KvEvent>[] runCommandHandlers = buildWorkHandlers(opts.getRunCommandWorker(), RunCommandHandler.class);
        this.preProcessDisruptor //
                .handleEventsWithWorkerPool(parseKeyHandlers) //
                .thenHandleEventsWithWorkerPool(detectHandlers);

        this.runCommandDisruptor //
                .handleEventsWithWorkerPool(runCommandHandlers);

        this.preProcessDisruptor.start();
        this.runCommandDisruptor.start();

        final ReadyTaskScheduler taskScheduler = new ReadyTaskScheduler(this.dagGraph);
        this.scheduler.scheduleAtFixedRate(taskScheduler, 0, 100, TimeUnit.MILLISECONDS);
        return true;
    }


    /**
     * Receive iterator from statemachine , deSerialize kvOperations, and send to preProcessor pipeline
     */
    public void dispatchBatch(final Iterator it) {
        final int batchSize = this.smrOptions.getTaskBatchSize();
        final RingBuffer<KvEvent> ringBuffer = this.preProcessDisruptor.getRingBuffer();
        RecyclableKvTask task = RecyclableKvTask.newInstance();
        List<KVState> kvStateList = task.getKvStateList();
        int cnt = 0;

        while (it.hasNext()) {
            if (cnt == batchSize) {
                dispatch(task, ringBuffer);
                cnt = 0;
                task = RecyclableKvTask.newInstance();
                kvStateList = task.getKvStateList();
            }

            KVOperation kvOp = null;
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
                    LOG.error("Error on serialize kvOp on index : {}, term: {}", it.getIndex(), it.getTerm());
                }
            }

            if (kvOp != null) {
                kvStateList.add(KVState.of(kvOp, done));
                cnt++;
            }
            it.next();
        }

        if (cnt > 0) {
            dispatch(task, ringBuffer);
        }
    }

    /**
     * Receive ready task from dagGraph, run it
     */
    public void dispatchReadyTask(final RecyclableKvTask readyTask) {
        dispatch(readyTask, this.runCommandDisruptor.getRingBuffer());
    }



    public void dispatch(final RecyclableKvTask task, final RingBuffer<KvEvent> ringBuffer) {
        try {
            final long sequence = ringBuffer.tryNext();
            try {
                final KvEvent kvEvent = ringBuffer.get(sequence);
                kvEvent.setTask(task);
            } finally {
                ringBuffer.publish(sequence);
            }
        } catch (final InsufficientCapacityException ignored) {
        }
    }

    @Override
    public void shutdown() {
        this.preProcessDisruptor.shutdown();
        this.scheduler.shutdown();
    }


    private Disruptor<KvEvent> buildDisruptor(final int bufferSize) {
        return  DisruptorBuilder.<KvEvent> newInstance()
                .setEventFactory(this.eventFactory)
                .setThreadFactory(new NamedThreadFactory("Rheakv-KvTaskPipeline-", true))
                .setRingBufferSize(bufferSize)
                .setProducerType(ProducerType.SINGLE)
                .setWaitStrategy(new BlockingWaitStrategy())
                .build();
    }

    private  WorkHandler<KvEvent>[] buildWorkHandlers(final int workerNums, final Class<?> handlerClass) {
        final WorkHandler<KvEvent>[] workHandlers = new WorkHandler[workerNums];
        for (int i = 0; i < workerNums; i++) {
            if (ParserKeyHandler.class == handlerClass) {
                workHandlers[i] = new ParserKeyHandler();
            } else if (DetectDependencyHandler.class == handlerClass) {
                workHandlers[i] = new DetectDependencyHandler(this.dagGraph);
            } else if (RunCommandHandler.class == handlerClass) {
                workHandlers[i] = new RunCommandHandler(this.stateMachine, this.rawKVStore);
            }
        }
        return workHandlers;
    }
}