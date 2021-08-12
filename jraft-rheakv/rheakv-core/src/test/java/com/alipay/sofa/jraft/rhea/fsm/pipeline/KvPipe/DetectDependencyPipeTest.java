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
import com.alipay.sofa.jraft.rhea.fsm.pipeline.PipeBaseTest;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author hzh (642256541@qq.com)
 */
public class DetectDependencyPipeTest extends PipeBaseTest {

    // Phase two
    private CalculateBloomFilterPipe       calculateBloomFilterPipe;

    // Phase three
    private DetectDependencyPipe           detectDependencyPipe;

    private DagTaskGraph<RecyclableKvTask> dagGraph;

    @Before
    public void init() {
        this.calculateBloomFilterPipe = new CalculateBloomFilterPipe();
        this.dagGraph = new DagTaskGraph<>();
        this.detectDependencyPipe = new DetectDependencyPipe(this.dagGraph);
    }

    @Test
    public void testDoProcess() {
        final RecyclableKvTask task1 = RecyclableKvTask.newInstance();
        final RecyclableKvTask task2 = RecyclableKvTask.newInstance();
        // Add some keys to task1
        {
            final List<KVState> kvStateList = task1.getKvStateList();
            kvStateList.add(mockKVState("key1"));
            kvStateList.add(mockKVState("key2"));
            this.calculateBloomFilterPipe.doProcess(task1);
        }
        // Add some keys to task2
        {
            final List<KVState> kvStateList = task2.getKvStateList();
            kvStateList.add(mockKVState("key2"));
            kvStateList.add(mockKVState("key3"));
            this.calculateBloomFilterPipe.doProcess(task2);
        }
        // Detect task1 and task2' s dependency
        {
            this.detectDependencyPipe.doProcess(task1);
            this.detectDependencyPipe.doProcess(task2);
            final Object[] readyTasks = this.dagGraph.getReadyTasks();
            Assert.assertEquals(1, readyTasks.length);
            Assert.assertEquals(task1, readyTasks[0]);
            this.dagGraph.notifyDone(task1);
        }
        {
            final Object[] readyTasks = this.dagGraph.getReadyTasks();
            Assert.assertEquals(task2, readyTasks[0]);
            this.dagGraph.notifyDone(task2);
        }
    }
}
