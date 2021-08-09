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
 * @author hzh (642256541@qq.com)
 */
public class DefaultPipelineTest {

    public static void main(String[] args) throws InterruptedException {
        final DefaultPipeline<String, String> pipeline = new DefaultPipeline<>();
        final Pipe<String, String> phaseOne = createPhaseOne();
        final Pipe<String, String> phaseTwo = createPhaseTwo();
        final Pipe<String, String> phaseThree = createPhaseThree();
        pipeline.addDisruptorBasedPipe(phaseOne, 2);
        pipeline.addDisruptorBasedPipe(phaseTwo, 2);
        pipeline.addDisruptorBasedPipe(phaseThree, 2);
        pipeline.init(newDefaultPipelineContext());
        System.out.println("Init pipeline done, begin to work");
        for (int i = 0; i < 8; i++) {
            pipeline.process("string { " + i + " }");
        }
        pipeline.shutdown(1000, TimeUnit.MILLISECONDS);
    }

    public static Pipe<String, String> createPhaseOne() {
        return new AbstractPipe<String, String>() {
            @Override
            public String doProcess(final String input) throws PipeException {
                System.out.println("Process message:" + input + " , current Pipe: pipe-one");
                try {
                    Thread.sleep(200);
                } catch (final InterruptedException ignored) {
                }
                return input + ", output1";
            }
        };
    }

    public static Pipe<String, String> createPhaseTwo() {
        return new AbstractPipe<String, String>() {
            @Override
            public String doProcess(final String input) throws PipeException {
                System.out.println("Process message:" + input + " , current Pipe: pipe-two");
                try {
                    Thread.sleep(200);
                } catch (final InterruptedException ignored) {
                }
                return input + ", output2";
            }
        };
    }

    public static Pipe<String, String> createPhaseThree() {
        return new AbstractPipe<String, String>() {
            @Override
            public String doProcess(final String input) throws PipeException {
                System.out.println("Process message:" + input + " , current Pipe: pipe-three");
                try {
                    Thread.sleep(200);
                } catch (final InterruptedException ignored) {
                }
                return input + ", output3";
            }
        };
    }

    public static PipeContext newDefaultPipelineContext() {
        return new PipeContext() {
            @Override
            public void handleError(final PipeException exp) {
                System.out.println("Error on handle pipe:" + exp);
            }
        };
    }
}