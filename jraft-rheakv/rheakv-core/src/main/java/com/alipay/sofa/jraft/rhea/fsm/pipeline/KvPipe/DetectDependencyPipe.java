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

import com.alipay.sofa.jraft.rhea.fsm.dag.DagTaskGraph;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.AbstractPipe;
import java.util.ArrayList;

/**
 * Detect the dependency between current batch and batches in DAGGraph
 * @author hzh (642256541@qq.com)
 */
public class DetectDependencyPipe extends AbstractPipe<RecyclableBatchWrapper, RecyclableBatchWrapper> {

    private final DagTaskGraph<RecyclableBatchWrapper> taskGraph;

    public DetectDependencyPipe(final DagTaskGraph<RecyclableBatchWrapper> taskGraph) {
        this.taskGraph = taskGraph;
    }

    @Override
    public RecyclableBatchWrapper doProcess(final RecyclableBatchWrapper batchWrapper) {
        return batchWrapper;
    }

    private boolean doDetect() {
        return true;
    }
}
