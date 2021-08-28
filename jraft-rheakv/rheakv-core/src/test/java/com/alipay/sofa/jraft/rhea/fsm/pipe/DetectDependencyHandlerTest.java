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

import com.alipay.sofa.jraft.rhea.fsm.ParallelPipeline.KvEvent;
import com.alipay.sofa.jraft.rhea.fsm.dag.DagGraph;
import com.alipay.sofa.jraft.rhea.fsm.dag.DefaultDagGraph;
import com.alipay.sofa.jraft.rhea.fsm.dag.GraphNode;
import com.alipay.sofa.jraft.rhea.fsm.pipe.DetectDependencyHandler.Detector;
import com.alipay.sofa.jraft.rhea.fsm.pipe.DetectDependencyHandler.RangeRelatedDetector;
import com.alipay.sofa.jraft.rhea.storage.KVClosureAdapter;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.storage.TestClosure;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author hzh (642256541@qq.com)
 */
public class DetectDependencyHandlerTest {

    private ParseKeyHandler            parseKeyHandler;

    private DetectDependencyHandler    detector;

    private DagGraph<RecyclableKvTask> dagGraph;

    @Before
    public void init() {
        this.parseKeyHandler = new ParseKeyHandler();
        this.dagGraph = new DefaultDagGraph<>();
        this.detector = new DetectDependencyHandler(this.dagGraph);
    }

    @Test
    public void testOnEvent() {
        final RecyclableKvTask task1 = RecyclableKvTask.newInstance();
        final RecyclableKvTask task2 = RecyclableKvTask.newInstance();

        final KvEvent kvEvent1 = new KvEvent();
        final KvEvent kvEvent2 = new KvEvent();
        // Add some keys to task1
        {
            final List<KVState> kvStateList = task1.getKvStateList();
            kvStateList.add(mockKVState("key1"));
            kvStateList.add(mockKVState("key2"));
            kvEvent1.setTask(task1);
            this.parseKeyHandler.onEvent(kvEvent1);
        }
        // Add some keys to task2
        {
            final List<KVState> kvStateList = task2.getKvStateList();
            kvStateList.add(mockKVState("key2"));
            kvStateList.add(mockKVState("key3"));
            kvEvent2.setTask(task2);
            this.parseKeyHandler.onEvent(kvEvent2);
        }
        // Detect task1 and task2' s dependency
        {
            this.detector.onEvent(kvEvent1);
            this.detector.onEvent(kvEvent2);
            final List<GraphNode<RecyclableKvTask>> readyTasks = this.dagGraph.getReadyNodes();
            Assert.assertEquals(1, readyTasks.size());
            final GraphNode<RecyclableKvTask> node = readyTasks.get(0);
            Assert.assertEquals(task1, node.getTask());
            this.dagGraph.notifyStart(node);
            this.dagGraph.notifyDone(node);
        }
        {
            final List<GraphNode<RecyclableKvTask>> readyTasks = this.dagGraph.getReadyNodes();
            final GraphNode<RecyclableKvTask> node = readyTasks.get(0);
            Assert.assertEquals(task2, node.getTask());
            this.dagGraph.notifyStart(node);
            this.dagGraph.notifyDone(node);
        }
    }

    @Test
    public void testRangeDetector() {
        Detector detector = new RangeRelatedDetector();
        final RecyclableKvTask task1 = RecyclableKvTask.newInstance();
        final RecyclableKvTask task2 = RecyclableKvTask.newInstance();

        // Add some keys to task1
        {
            task1.setMinKey("key2");
            task1.setMaxKey("key3");
        }
        {
            task2.addRangeKeyPair(Pair.of("key1", "key5"));
        }
        Assert.assertTrue(detector.doDetect(task1, task2));
    }

    public KVState mockKVState(final String key) {
        final KVOperation op = KVOperation.createPut(BytesUtil.writeUtf8(key), BytesUtil.writeUtf8(key));
        final KVClosureAdapter adapter = new KVClosureAdapter(new TestClosure(), op);
        return KVState.of(op, adapter);
    }
}