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

import com.alipay.sofa.jraft.rhea.fsm.dag.GraphNode.TaskStatus;
import com.alipay.sofa.jraft.rhea.util.Lists;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Use lock to ensure concurrent security
 * @author hzh (642256541@qq.com)
 */
public class DefaultDagGraph<Item> implements DagGraph<Item> {

    private final Set<GraphNode<Item>> graph;
    private final ReentrantLock        lock;

    public DefaultDagGraph() {
        this.graph = new HashSet<>();
        this.lock = new ReentrantLock();
    }

    @Override
    public void addNode(final GraphNode<Item> node) {
        this.lock.lock();
        try {
            this.graph.add(node);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void addNode(final GraphNode<Item> childNode, final List<GraphNode<Item>> parentNodes) {
        this.lock.lock();
        try {
            this.graph.add(childNode);
            for (final GraphNode<Item> parentNode : parentNodes) {
                if (this.graph.contains(parentNode) && !parentNode.isDone()) {
                    parentNode.addChild(childNode);
                    childNode.addParent(parentNode);
                    childNode.setTaskStatus(TaskStatus.BLOCKING);
                }
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public List<GraphNode<Item>> getReadyNodes() {
        this.lock.lock();
        try {
            final List<GraphNode<Item>> result = new ArrayList<>(this.graph.size());
            for (final GraphNode<Item> node : this.graph) {
                if (node.isWaiting()) {
                    result.add(node);
                }
            }
            return result;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public List<GraphNode<Item>> getAllNodes() {
        this.lock.lock();
        try {
            final Iterator<GraphNode<Item>> iterator = this.graph.iterator();
            return Lists.newArrayList(iterator);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void notifyStart(final GraphNode<Item> node) {
        this.lock.lock();
        try {
            if (this.graph.contains(node) && node.isWaiting()) {
                node.setTaskStatus(TaskStatus.RUNNING);
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void notifyDone(final GraphNode<Item> node) {
        this.lock.lock();
        try {
            if (this.graph.contains(node) && node.isRunning()) {
                node.setTaskStatus(TaskStatus.DONE);
                node.notifyChildren();
                this.graph.remove(node);
            }
        } finally {
            this.lock.unlock();
        }
    }
}
