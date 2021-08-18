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
package com.alipay.sofa.jraft.rhea.fsm.pipe;

import com.alipay.sofa.jraft.rhea.fsm.dag.DagTaskGraph;
import com.alipay.sofa.jraft.rhea.fsm.pipe.RecyclableKvTask.TaskStatus;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.util.BloomFilter;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.lmax.disruptor.WorkHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hzh (642256541@qq.com)
 */
public class DetectDependencyHandler implements WorkHandler<KvEvent> {

    private final DagTaskGraph<RecyclableKvTask> taskGraph;

    public DetectDependencyHandler(final DagTaskGraph<RecyclableKvTask> taskGraph) {
        this.taskGraph = taskGraph;
    }

    @Override
    public void onEvent(final KvEvent event) {
        final long begin = System.currentTimeMillis();
        final RecyclableKvTask task = event.getTask();
        final List<RecyclableKvTask> preTasks = this.taskGraph.getAllTasks();
        final List<RecyclableKvTask> dependentTasks = new ArrayList<>(preTasks.size());
        // Find out every pre task that this batch depends on
        for (final RecyclableKvTask preTask : preTasks) {
            final TaskStatus taskStatus = preTask.getTaskStatus();
            if (taskStatus.equals(TaskStatus.DONE) || taskStatus.equals(TaskStatus.INIT)) {
                continue;
            }
            if (doDetect(task, preTask)) {
                dependentTasks.add(preTask);
            }
        }
        task.setTaskStatus(TaskStatus.WAITING);
        // Add all edges to dagGraph
        this.taskGraph.add(task, dependentTasks);
        //System.out.println("detect handler:" + task);
        System.out.println("detect pipe , cost :" + (System.currentTimeMillis() - begin));
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
            CalculateBloomFilterHandler.doGetOPKey(kvOp, waitToCheckKeyList);
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
