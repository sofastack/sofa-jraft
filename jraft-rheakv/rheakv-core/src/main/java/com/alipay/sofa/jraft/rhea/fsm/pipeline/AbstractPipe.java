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

import java.util.concurrent.TimeUnit;

/**
 * Process input, pass the output to the next pipe
 * @author hzh (642256541@qq.com)
 */
public abstract class AbstractPipe<IN, OUT> implements Pipe<IN, OUT> {

    protected Pipe<?, ?>  nextPhase;
    protected PipeContext pipeContext;

    public void init(final PipeContext pipeException) {
        this.pipeContext = pipeContext;
    }

    public void nextPipe(final Pipe<?, ?> pipe) {
        this.nextPhase = pipe;
    }

    public void shutdown(final long timeout, final TimeUnit timeUnit) {
    }

    public abstract OUT doProcess(final IN input) throws PipeException;

    public void process(final IN input) throws InterruptedException {
        try {
            final OUT output = this.doProcess(input);
            if (output != null && nextPhase != null) {
                ((Pipe<OUT, ?>) nextPhase).process(output);
            }
            ;
        } catch (final PipeException pipeException) {
            this.pipeContext.handleError(pipeException);
        }
    }
}
