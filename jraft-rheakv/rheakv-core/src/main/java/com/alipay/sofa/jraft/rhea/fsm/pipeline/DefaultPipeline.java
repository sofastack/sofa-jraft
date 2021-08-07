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
package com.alipay.sofa.jraft.rhea.fsm.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hzh (642256541@qq.com)
 */
public class DefaultPipeline<IN, OUT> extends AbstractPipe<IN, OUT> implements Pipeline<IN, OUT> {

    private final static Logger          LOG          = LoggerFactory.getLogger(DefaultPipeline.class);
    private final LinkedList<Pipe<?, ?>> pipes        = new LinkedList<>();
    private final ExecutorService        helpExecutor = Executors.newSingleThreadExecutor();

    public DefaultPipeline() {
    }

    @Override
    public void addPipe(final Pipe<?, ?> pipe) {
        this.pipes.add(pipe);
    }

    public <INPUT, OUTPUT> void addDisruptorBasedPipe(final Pipe<INPUT, OUTPUT> pipe, final int bufferSize) {
        final DisruptorBasedPipeDecorator<INPUT, OUTPUT> disruptorPipe = new DisruptorBasedPipeDecorator<>(pipe,
            bufferSize);
        this.addPipe(disruptorPipe);
    }

    @Override
    public void init(final PipeContext pipeContext) {
        final LinkedList<Pipe<?, ?>> pipes = this.pipes;
        Pipe<?, ?> prevPipe = this;
        for (final Pipe<?, ?> pipe : pipes) {
            prevPipe.nextPipe(pipe);
            prevPipe = pipe;
        }
        for (Pipe<?, ?> pipe : pipes) {
            pipe.init(pipeContext);
        }
    }

    @Override
    public void process(final IN input) throws InterruptedException {
        if (this.pipes.size() > 0) {
            final Pipe<IN, ?> firstPipe = (Pipe<IN, ?>) this.pipes.peek();
            firstPipe.process(input);
        }
    }

    @Override
    public void shutdown(final long timeout, final TimeUnit timeUnit) {
        Pipe<?, ?> pipe = null;
        while ((pipe = this.pipes.poll()) != null) {
            pipe.shutdown(timeout, timeUnit);
        }
        this.helpExecutor.shutdown();
    }

    public PipeContext newDefaultPipeContext () {
        return exp -> helpExecutor.submit(() -> {
            // do sth
            LOG.error("Error on handle object {} on pipe {}", exp.input, exp.sourcePipe, exp.getCause());
        });
    }

    @Override
    public OUT doProcess(final IN input) {
        return null;
    }
}
