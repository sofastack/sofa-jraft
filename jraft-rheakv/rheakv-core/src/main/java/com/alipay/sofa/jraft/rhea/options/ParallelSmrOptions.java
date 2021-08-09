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

/**
 *
 * @author hzh (642256541@qq.com)
 */
public class ParallelSmrOptions {

    // Nums of disruptor
    private int ReadKVOperationPipeWorkerNums      = 1;

    private int CalculateBloomFilterPipeWorkerNums = 1;

    private int DetectDependencyPipeWorkerNums     = 2;

    public int getReadKVOperationPipeWorkerNums() {
        return ReadKVOperationPipeWorkerNums;
    }

    public void setReadKVOperationPipeWorkerNums(final int readKVOperationPipeWorkerNums) {
        ReadKVOperationPipeWorkerNums = readKVOperationPipeWorkerNums;
    }

    public int getCalculateBloomFilterPipeWorkerNums() {
        return CalculateBloomFilterPipeWorkerNums;
    }

    public void setCalculateBloomFilterPipeWorkerNums(final int calculateBloomFilterPipeWorkerNums) {
        CalculateBloomFilterPipeWorkerNums = calculateBloomFilterPipeWorkerNums;
    }

    public int getDetectDependencyPipeWorkerNums() {
        return DetectDependencyPipeWorkerNums;
    }

    public void setDetectDependencyPipeWorkerNums(final int detectDependencyPipeWorkerNums) {
        DetectDependencyPipeWorkerNums = detectDependencyPipeWorkerNums;
    }
}
