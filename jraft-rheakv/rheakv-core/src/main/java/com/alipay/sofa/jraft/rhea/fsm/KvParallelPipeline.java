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
import com.alipay.sofa.jraft.rhea.fsm.pipe.CalculateBloomFilterHandler;
import com.alipay.sofa.jraft.rhea.fsm.pipe.DetectDependencyHandler;
import com.alipay.sofa.jraft.rhea.fsm.pipe.KvEvent;
import com.alipay.sofa.jraft.rhea.fsm.pipe.RecyclableKvTask;
import com.alipay.sofa.jraft.rhea.fsm.pipe.RecyclableKvTask.TaskStatus;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.fsm.dag.DagTaskGraph;
import com.alipay.sofa.jraft.rhea.fsm.pipe.RunOperationHandler;
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
 * Pipeline of parallel kv statemachine, base on disruptor
 * First (preProcessDisruptor):
 * calculate bloom filter -> detect dependency
 *
 * Second (dagGraph):
 * add to dagGraph, wait to be scheduled
 *
 * Third (timer + runOperationDisruptor):
 * timer schedules ready tasks to runOperationDisruptor
 */
public class KvParallelPipeline implements Lifecycle<ParallelSmrOptions> {

    private static final Logger LOG  = LoggerFactory.getLogger(KvParallelPipeline.class);

    private final DagTaskGraph<RecyclableKvTask> dagTaskGraph;

    private final BaseRawKVStore<?> rawKVStore;
    private final ParallelKVStateMachine stateMachine;

    private final Serializer serializer   = Serializers.getDefault();

    private  ParallelSmrOptions     smrOptions;

    private Disruptor<KvEvent> preProcessDisruptor;

    private Disruptor<KvEvent> runOperationDisruptor;

    private final ScheduledExecutorService dagScheduler = Executors.newSingleThreadScheduledExecutor();


    private final EventFactory<KvEvent> eventFactory = KvEvent::new;


    public KvParallelPipeline(final DagTaskGraph<RecyclableKvTask> dagTaskGraph, final ParallelKVStateMachine stateMachine, final BaseRawKVStore<?> rawKVStore) {
        this.dagTaskGraph = dagTaskGraph;
        this.stateMachine = stateMachine;
        this.rawKVStore = rawKVStore;
    }


    @Override
    public boolean init(final ParallelSmrOptions opts) {
        this.smrOptions = opts;
        final int preProcessBufferSize = opts.getPreprocessBufferSize();
        final int runOperationBufferSize = opts.getRunOperationBufferSize();

        this.preProcessDisruptor = this.buildDisruptor(preProcessBufferSize);
        this.runOperationDisruptor = this.buildDisruptor(runOperationBufferSize);

        // Pipeline , Each handler is a thread pool
        this.preProcessDisruptor //
                .handleEventsWithWorkerPool(buildCalculatorHandlers(opts.getCalculatorWorker())) //
                .thenHandleEventsWithWorkerPool(buildDetectorHandlers(opts.getDetectorWorker()));
        this.preProcessDisruptor.start();

        this.runOperationDisruptor //
                .handleEventsWithWorkerPool(buildRunOperationHandlers(opts.getRunOperationWorker()));
        this.runOperationDisruptor.start();

        // Get ready tasks from dag graph, and run it
        this.dagScheduler.scheduleAtFixedRate(() -> {
            final List<RecyclableKvTask> readyTasks = this.dagTaskGraph.getReadyTasks();
            for (final RecyclableKvTask kvTask : readyTasks) {
                if (!kvTask.getTaskStatus().equals(RecyclableKvTask.TaskStatus.WAITING)) {
                    continue;
                }
                //System.out.println("scheduler dispatch task:" + kvTask);
                kvTask.setTaskStatus(TaskStatus.RUNNING);
                kvTask.setDone((status) -> {
                    kvTask.setTaskStatus(TaskStatus.DONE);
                    dagTaskGraph.notifyDone(kvTask);
                    kvTask.recycle();
                });
                this.dagTaskGraph.notifyStart(kvTask);
                this.dispatchReadyTask(kvTask);
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
        return true;
    }


    /**
     * Receive iterator from statemachine , deSerialize kvOperations, and send to preProcessor pipeline
     */
    public void dispatchIterator(final Iterator it) {
        final int batchSize = this.smrOptions.getTaskBatchSize();
        final long begin = System.currentTimeMillis();
        final RingBuffer<KvEvent> ringBuffer = this.preProcessDisruptor.getRingBuffer();
        RecyclableKvTask task = RecyclableKvTask.newInstance();
        List<KVState> kvStateList = task.getKvStateList();
        int cnt = 0;
        while (it.hasNext()) {
            if (cnt == batchSize) {
                this.dispatch(task, ringBuffer);
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
            this.dispatch(task, ringBuffer);
        }
        System.out.println("reader pipe , cost :" + (System.currentTimeMillis() - begin));
    }

    /**
     * Receive ready task from dagGraph, run it
     */
    public void dispatchReadyTask(final RecyclableKvTask readyTask) {
        this.dispatch(readyTask, this.runOperationDisruptor.getRingBuffer());
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
        this.dagScheduler.shutdown();
    }


    private Disruptor<KvEvent> buildDisruptor(final int bufferSize) {
        return  DisruptorBuilder.<KvEvent> newInstance()
                .setEventFactory(this.eventFactory)
                .setThreadFactory(new NamedThreadFactory("Rheakv-KvTaskPreprocessPipeline-", true))
                .setRingBufferSize(bufferSize)
                .setProducerType(ProducerType.SINGLE)
                .setWaitStrategy(new BlockingWaitStrategy())
                .build();
    }


    private WorkHandler<KvEvent>[] buildCalculatorHandlers(final int workerNums) {
        WorkHandler<KvEvent>[] workHandlers = new WorkHandler[workerNums];
        for (int i = 0; i < workerNums; i++) {
            workHandlers[i] = new CalculateBloomFilterHandler();
        }
        return workHandlers;
    }

    private WorkHandler<KvEvent>[] buildDetectorHandlers(final int workerNums) {
        WorkHandler<KvEvent>[] workHandlers = new WorkHandler[workerNums];
        for (int i = 0; i < workerNums; i++) {
            workHandlers[i] = new DetectDependencyHandler(this.dagTaskGraph);
        }
        return workHandlers;
    }

    private WorkHandler<KvEvent>[] buildRunOperationHandlers(final int workerNums) {
        WorkHandler<KvEvent>[] workHandlers = new WorkHandler[workerNums];
        for (int i = 0; i < workerNums; i++) {
            workHandlers[i] = new RunOperationHandler(this.stateMachine, this.rawKVStore);
        }
        return workHandlers;
    }
}