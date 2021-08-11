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
package com.alipay.sofa.jraft.rhea.fsm.pipeline.KvPipe;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.fsm.dag.DagTaskGraph;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.DefaultPipeline;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.DisruptorBasedPipeDecorator;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.Pipe;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.PipeContext;
import com.alipay.sofa.jraft.rhea.options.ParallelSmrOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hzh (642256541@qq.com)
 */
public class KvTaskPipeline implements Lifecycle<ParallelSmrOptions> {
    private static final Logger                         LOG          = LoggerFactory.getLogger(KvTaskPipeline.class);

    private DefaultPipeline<Iterator, RecyclableKvTask> taskPipeline;
    private final DagTaskGraph<RecyclableKvTask>        dagTaskGraph;
    private final AtomicBoolean                         started      = new AtomicBoolean(false);
    private final ExecutorService                       helpExecutor = Executors.newSingleThreadExecutor();
    private PipeContext                                 pipeContext;

    public KvTaskPipeline(final DagTaskGraph<RecyclableKvTask> dagTaskGraph) {
        this.dagTaskGraph = dagTaskGraph;
    }

    @Override
    public boolean init(final ParallelSmrOptions opts) {
        if (!this.started.compareAndSet(false, true)) {
            return false;
        }
        this.taskPipeline = new DefaultPipeline<>();
        final Pipe<Iterator, RecyclableKvTask> readTaskPipe = new ReaderPipe();
        final Pipe<RecyclableKvTask, RecyclableKvTask> calculateBloomPipe = new CalculateBloomFilterPipe();
        final Pipe<RecyclableKvTask, RecyclableKvTask> detectDependencyPipe = new DetectDependencyPipe(
            this.dagTaskGraph);
        this.taskPipeline.addDisruptorBasedPipe(readTaskPipe, opts.getReaderPipeWorkerNums());
        this.taskPipeline.addDisruptorBasedPipe(calculateBloomPipe, opts.getCalculateBloomFilterPipeWorkerNums());
        this.taskPipeline.addDisruptorBasedPipe(detectDependencyPipe, opts.getDetectDependencyPipeWorkerNums());
        this.pipeContext = this.newDefaultPipeContext();
        this.taskPipeline.init(this.pipeContext);
        LOG.info("KvTaskPipeline init success");
        return true;
    }

    /**
     * Send iterator to pipeline , wait to be processed
     */
    public void process(final Iterator iterator) throws InterruptedException {
        while (iterator.hasNext()) {
            this.taskPipeline.process(iterator);
        }
    }

    public PipeContext newDefaultPipeContext () {
        return exp -> helpExecutor.submit(() -> {
            // todo : try task again?
            LOG.error("Error on schedule task {} on pipe {}", exp.input, exp.sourcePipe, exp.getCause());
        });
    }

    @Override
    public void shutdown() {
        if (!this.started.compareAndSet(true, false)) {
            return;
        }
        this.taskPipeline.shutdown(1000, TimeUnit.MILLISECONDS);
    }

    public PipeContext getPipeContext() {
        return pipeContext;
    }
}
