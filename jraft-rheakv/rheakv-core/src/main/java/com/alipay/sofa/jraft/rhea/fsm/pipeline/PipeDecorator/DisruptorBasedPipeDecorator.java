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
package com.alipay.sofa.jraft.rhea.fsm.pipeline.PipeDecorator;

import com.alipay.sofa.jraft.rhea.fsm.pipeline.Pipe;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.PipeContext;
import com.alipay.sofa.jraft.util.DisruptorBuilder;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pipe base on disruptor
 * @author hzh (642256541@qq.com)
 */
public class DisruptorBasedPipeDecorator<IN, OUT> implements Pipe<IN, OUT> {

    private final static Logger       LOG                   = LoggerFactory
                                                                .getLogger(DisruptorBasedPipeDecorator.class);

    private final int                 MAX_APPLY_RETRY_TIMES = 3;
    private final Pipe<IN, OUT>       delegate;
    private final int                 bufferSize;
    private Disruptor<PipeEvent<IN>>  disruptor;
    private RingBuffer<PipeEvent<IN>> ringBuffer;
    private final AtomicBoolean       start                 = new AtomicBoolean(false);

    private static class PipeEvent<IN> {
        IN input;

        public void reset() {
            input = null;
        }
    }

    public DisruptorBasedPipeDecorator(Pipe<IN, OUT> delegate, final int bufferSize) {
        this.delegate = delegate;
        this.bufferSize = bufferSize;

    }

    @Override
    public void process(final IN input) {
        int retryTimes = 0;
        final EventTranslator<PipeEvent<IN>> translator = (event, sequence) ->{
            event.reset();
            event.input = input;
        };
        while (true) {
            if (this.ringBuffer.tryPublishEvent(translator)) {
                break;
            } else {
                retryTimes++;
                if (retryTimes > MAX_APPLY_RETRY_TIMES) {
                    LOG.warn("Error on publish event :{}", input);
                    return;
                }
            }
        }
    }

    @Override
    public void init(final PipeContext pipeException) {
        if (!start.compareAndSet(false, true)) {
            return;
        }
        this.delegate.init(pipeException);
        this.disruptor = DisruptorBuilder.<PipeEvent<IN>> newInstance()
                .setEventFactory(PipeEvent::new)
                .setThreadFactory(new NamedThreadFactory("Rheakv-DisruptorBasedPipe-", true))
                .setRingBufferSize(bufferSize)
                .setProducerType(ProducerType.SINGLE)
                .setWaitStrategy(new BlockingWaitStrategy())
                .build();
        this.disruptor.handleEventsWith((pipeEvent, l, b) -> delegate.process(pipeEvent.input));
        this.ringBuffer = this.disruptor.start();
    }

    @Override
    public void shutdown(final long timeout, final TimeUnit timeUnit) {
        if (!start.compareAndSet(true, false)) {
            return;
        }
        this.disruptor.shutdown();
    }

    @Override
    public void nextPipe(final Pipe<?, ?> pipe) {
        this.delegate.nextPipe(pipe);
    }
}
