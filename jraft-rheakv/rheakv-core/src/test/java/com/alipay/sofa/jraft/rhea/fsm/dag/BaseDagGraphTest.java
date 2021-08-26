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
package com.alipay.sofa.jraft.rhea.fsm.dag;

import com.alipay.sofa.jraft.rhea.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author hzh (642256541@qq.com)
 */
public class BaseDagGraphTest {
    private DagGraph<String> dagGraph;

    @Before
    public void setup() {
        this.dagGraph = new DefaultDagGraph<>();
    }

    @Test
    public void testConcurrent() throws InterruptedException {
        final Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            final int finalI = i;
            threads[i] = new Thread(() -> {
                final GraphNode<String> node = new GraphNode<>("key-" + finalI);
                this.dagGraph.addNode(node);
            });
            threads[i].start();
        }
        for (int i = 0; i < 10; i++) {
            threads[i].join();
        }
        final List<GraphNode<String>> readyNodes = this.dagGraph.getReadyNodes();
        Assert.assertEquals(10, readyNodes.size());

        for (int i = 0; i < 5; i++) {
            this.dagGraph.notifyStart(readyNodes.get(i));
        }

        final List<GraphNode<String>> readyNodes1 = this.dagGraph.getReadyNodes();
        Assert.assertEquals(5, readyNodes1.size());
    }

    @Test
    public void testDependency() {
        final GraphNode<String> node = new GraphNode<>("key1");
        final GraphNode<String> node1 = new GraphNode<>("key2");
        this.dagGraph.addNode(node);
        this.dagGraph.addNode(node1, Lists.newArrayList(node));
        final List<GraphNode<String>> readyNodes = this.dagGraph.getReadyNodes();
        Assert.assertEquals(1, readyNodes.size());
        Assert.assertEquals(node, readyNodes.get(0));

        this.dagGraph.notifyStart(node);
        this.dagGraph.notifyDone(node);

        final List<GraphNode<String>> readyNodes1 = this.dagGraph.getReadyNodes();
        Assert.assertEquals(1, readyNodes.size());
        Assert.assertEquals(node1, readyNodes1.get(0));
    }

}