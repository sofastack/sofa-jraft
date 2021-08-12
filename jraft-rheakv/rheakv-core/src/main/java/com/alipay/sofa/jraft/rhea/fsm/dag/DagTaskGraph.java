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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.List;

/**
 * @author hzh (642256541@qq.com)
 */
public class DagTaskGraph<Item> {
    private final DirectedGraph<Item, DefaultEdge> graph          = new DefaultDirectedGraph<>(DefaultEdge.class);
    // todo: Find a more appropriate data structure
    private final CopyOnWriteArraySet<Item>        schedulingTask = new CopyOnWriteArraySet<>();
    private final ReentrantReadWriteLock           readWriteLock  = new ReentrantReadWriteLock();
    private final Lock                             readLock       = readWriteLock.readLock();
    private final Lock                             writeLock      = readWriteLock.writeLock();

    public DagTaskGraph() {
    }

    public DagTaskGraph<Item> add(final Item childTask, final List<Item> parentTasks) {
        this.writeLock.lock();
        try {
            this.graph.addVertex(childTask);
            this.schedulingTask.add(childTask);
            for (final Item parentTask : parentTasks) {
                if (this.schedulingTask.contains(parentTask)) {
                    this.graph.addEdge(parentTask, childTask);
                }
            }
        } finally {
            this.writeLock.unlock();
        }
        return this;
    }

    public DagTaskGraph<Item> add(final Item childTask, final Item... parentTasks) {
        return this.add(childTask, Arrays.asList(parentTasks));
    }

    public boolean isDone() {
        this.readLock.lock();
        try {
            return this.graph.vertexSet().isEmpty();
        } finally {
            this.readLock.unlock();
        }
    }

    public  Object[] getReadyTasks() {
        this.readLock.lock();
        try {
            return this.graph.vertexSet().stream()
                    .filter(task -> this.graph.inDegreeOf(task) == 0)
                    .toArray(Object[]::new);
        } finally {
            this.readLock.unlock();
        }
    }

    public Iterator<Item> getAllTasks() {
        this.readLock.lock();
        try {
            return this.schedulingTask.iterator();
        } finally {
            this.readLock.unlock();
        }
    }

    public void notifyDone(final Item task) {
        this.writeLock.lock();
        try {
            Requires.requireNonNull(task);
            if (!this.schedulingTask.contains(task)) {
                return;
            }
            this.schedulingTask.remove(task);
            this.graph.removeVertex(task);
        } finally {
            this.writeLock.unlock();
        }
    }
}
