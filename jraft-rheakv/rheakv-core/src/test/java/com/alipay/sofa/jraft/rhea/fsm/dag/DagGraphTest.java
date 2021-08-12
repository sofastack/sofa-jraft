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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author hzh (642256541@qq.com)
 */
public class DagGraphTest {
    private DagTaskGraph<String> graph;

    @Test
    public void testAddAndGet() {
        this.graph = new DagTaskGraph<>();
        final String s1 = "item1";
        final String s2 = "item2";
        final String s3 = "item3";
        final String s4 = "item4";
        graph.add(s1).add(s2, s1).add(s4, s2).add(s3, s2);
        {
            final Object[] readyItem = graph.getReadyTasks();
            assertEquals(readyItem.length, 1);
            assertEquals(s1, readyItem[0]);
            graph.notifyDone(s1);
        }
        {
            final Object[] readyItem = graph.getReadyTasks();
            assertEquals(readyItem.length, 1);
            assertEquals(s2, readyItem[0]);
            graph.notifyDone(s2);
        }
        {
            final Object[] readyItems = graph.getReadyTasks();
            assertEquals(readyItems.length, 2);
        }
    }
}