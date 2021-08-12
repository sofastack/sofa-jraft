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
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.util.BloomFilter;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.rhea.fsm.pipeline.KvPipe.RecyclableKvTask.TaskStatus;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Detect the dependency between current batch and batches in dagGraph
 * @author hzh (642256541@qq.com)
 */
public class DetectDependencyPipe extends AbstractPipe<RecyclableKvTask, RecyclableKvTask> {

    private final DagTaskGraph<RecyclableKvTask> taskGraph;

    public DetectDependencyPipe(final DagTaskGraph<RecyclableKvTask> taskGraph) {
        this.taskGraph = taskGraph;
    }

    @Override
    public RecyclableKvTask doProcess(final RecyclableKvTask task) {
        final long begin = System.currentTimeMillis();
        final Iterator<RecyclableKvTask> preWaitingTasks = this.taskGraph.getAllTasks();
        final ArrayList<RecyclableKvTask> dependentTasks = new ArrayList<>();
        // Find out every pre batch that this batch depends on
        while (preWaitingTasks.hasNext()) {
            final RecyclableKvTask preTask = preWaitingTasks.next();
            if (preTask.getTaskStatus() != TaskStatus.DONE && doDetect(task, preTask)) {
                dependentTasks.add(preTask);
            }
        }
        // Add all edges to dagGraph
        task.setTaskStatus(TaskStatus.WAITING);
        this.taskGraph.add(task, dependentTasks);
        System.out.println("detect pipe , cost :" + (System.currentTimeMillis() - begin));
        return task;
    }

    /**
     * Detect whether two batch has dependency
     * todo:
     * 1.childBatch have merge/scan , parentBatch don't have
     * 2.childBatch have merge/scan , parentBatch have too
     * 3.childBatch dont't have merge/scan, parentBatch have
     */
    public static boolean doDetect(final RecyclableKvTask childTask, final RecyclableKvTask parentTask) {
        final List<KVState> childKVStateList = childTask.getKvStateList();
        final BloomFilter<byte[]> parentBloomFilter = parentTask.getFilter();
        final ArrayList<byte[]> waitToCheckKeyList = new ArrayList<>(childKVStateList.size());
        for (final KVState kvState : childKVStateList) {
            final KVOperation kvOp = kvState.getOp();
            CalculateBloomFilterPipe.doGetOPKey(kvOp, waitToCheckKeyList);
        }
        // Check whether have same key
        for (final byte[] key : waitToCheckKeyList) {
            if (parentBloomFilter.contains(key)) {
                System.out.println("the key is same:" + BytesUtil.readUtf8(key));
                return true;
            }
        }
        return false;
    }
}
