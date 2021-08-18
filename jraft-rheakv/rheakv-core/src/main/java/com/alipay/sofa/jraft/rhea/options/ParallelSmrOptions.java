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

    private int CalculatorWorker       = Utils.cpus() / 2;

    private int DetectorWorker         = Utils.cpus() / 2;

    private int PreprocessBufferSize   = Utils.cpus() / 2 << 4;

    private int RunOperationWorker     = Utils.cpus();

    private int RunOperationBufferSize = Utils.cpus() << 4;

    private int TaskBatchSize          = 10;

    public int getRunOperationBufferSize() {
        return RunOperationBufferSize;
    }

    public void setRunOperationBufferSize(final int runOperationBufferSize) {
        RunOperationBufferSize = runOperationBufferSize;
    }

    public int getTaskBatchSize() {
        return TaskBatchSize;
    }

    public void setTaskBatchSize(final int taskBatchSize) {
        TaskBatchSize = taskBatchSize;
    }

    public int getPreprocessBufferSize() {
        return PreprocessBufferSize;
    }

    public void setPreprocessBufferSize(final int preprocessBufferSize) {
        PreprocessBufferSize = preprocessBufferSize;
    }

    public int getRunOperationWorker() {
        return RunOperationWorker;
    }

    public void setRunOperationWorker(final int runOperationWorker) {
        RunOperationWorker = runOperationWorker;
    }

    public int getCalculatorWorker() {
        return CalculatorWorker;
    }

    public void setCalculatorWorker(final int calculatorWorker) {
        CalculatorWorker = calculatorWorker;
    }

    public int getDetectorWorker() {
        return DetectorWorker;
    }

    public void setDetectorWorker(final int detectorWorker) {
        DetectorWorker = detectorWorker;
    }
}
