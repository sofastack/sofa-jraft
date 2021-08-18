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

import com.alipay.sofa.jraft.util.Requires;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;

/**
 * Dag graph that stores scheduling tasks
 * @author hzh (642256541@qq.com)
 */
public class DagTaskGraph<Item> {
    private final DirectedGraph<Item, DefaultEdge> graph       = new DefaultDirectedGraph<>(DefaultEdge.class);
    private final Set<Item>                        runningTask = new HashSet<>();
    private final StampedLock                      stampedLock = new StampedLock();

    public DagTaskGraph() {
    }

    public DagTaskGraph<Item> add(final Item childTask, final List<Item> parentTasks) {
        final long stamp = this.stampedLock.writeLock();
        try {
            this.graph.addVertex(childTask);
            for (final Item parentTask : parentTasks) {
                if (this.graph.vertexSet().contains(parentTask)) {
                    this.graph.addEdge(parentTask, childTask);
                }
            }
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
        return this;
    }

    public DagTaskGraph<Item> add(final Item childTask, final Item... parentTasks) {
        return this.add(childTask, Arrays.asList(parentTasks));
    }

    /**
     * @return Ready tasks : the tasks that inDegree = 0
     */
    public List<Item> getReadyTasks() {
        long stamp = this.stampedLock.tryOptimisticRead();
        List<Item> result = this.filterReadyTasks();
        if (!this.stampedLock.validate(stamp)) {
            stamp = this.stampedLock.readLock();
            try {
                result = this.filterReadyTasks();
            } finally {
                this.stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }

    /**
     * @return All tasks
     */
    public List<Item> getAllTasks() {
        long stamp = this.stampedLock.tryOptimisticRead();
        List<Item> result = this.copyFromIterator(this.graph.vertexSet().iterator());
        if (!this.stampedLock.validate(stamp)) {
            stamp = this.stampedLock.readLock();
            try {
                result = this.copyFromIterator(this.graph.vertexSet().iterator());
            } finally {
                this.stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }

    public void notifyStart(final Item task) {
        final long stamp = this.stampedLock.writeLock();
        try {
            Requires.requireNonNull(task);
            if (!this.graph.vertexSet().contains(task) || this.runningTask.contains(task)) {
                return;
            }
            this.runningTask.add(task);
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
    }

    public void notifyDone(final Item task) {
        final long stamp = this.stampedLock.writeLock();
        try {
            Requires.requireNonNull(task);
            if (!this.runningTask.contains(task)) {
                return;
            }
            this.runningTask.remove(task);
            this.graph.removeVertex(task);
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
    }

    private List<Item> filterReadyTasks() {
        final Iterator<Item> iterator = this.graph.vertexSet().stream()
                .filter(task -> !this.runningTask.contains(task) &&
                        this.graph.inDegreeOf(task) == 0)
                .iterator();
        return this.copyFromIterator(iterator);
    }

    private List<Item> copyFromIterator(final Iterator<Item> iterator) {
        final ArrayList<Item> items = new ArrayList<>();
        while (iterator.hasNext()) {
            items.add(iterator.next());
        }
        return items;
    }
}
