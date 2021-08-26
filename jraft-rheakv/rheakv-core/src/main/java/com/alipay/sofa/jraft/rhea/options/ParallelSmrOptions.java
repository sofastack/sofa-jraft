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
package com.alipay.sofa.jraft.rhea.options;

import com.alipay.sofa.jraft.util.Utils;

/**
 *
 * @author hzh (642256541@qq.com)
 */
public class ParallelSmrOptions {

    private int       ParserWorker       = Utils.cpus() >> 1;

    private int       DetectorWorker     = Utils.cpus() >> 1;

    private int       RunCommandWorker   = Utils.cpus();

    private int       PipelineBufferSize = Utils.cpus() << 5;

    public static int TaskBatchSize      = 50;

    public void setPipelineBufferSize(final int pipelineBufferSize) {
        this.PipelineBufferSize = pipelineBufferSize;
    }

    public int getPipelineBufferSize() {
        return this.PipelineBufferSize;
    }

    public int getTaskBatchSize() {
        return TaskBatchSize;
    }

    public int getRunCommandWorker() {
        return this.RunCommandWorker;
    }

    public void setRunCommandWorker(final int runCommandWorker) {
        this.RunCommandWorker = runCommandWorker;
    }

    public int getParserWorker() {
        return this.ParserWorker;
    }

    public void setParserWorker(final int parserWorker) {
        this.ParserWorker = parserWorker;
    }

    public int getDetectorWorker() {
        return this.DetectorWorker;
    }

    public void setDetectorWorker(final int detectorWorker) {
        this.DetectorWorker = detectorWorker;
    }
}
