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

import java.util.HashSet;
import java.util.Set;

/**
 * Note that none of graphNode's functions are atomic; need to ensure atomic at the level of DagGraph
 * @author hzh (642256541@qq.com)
 */
public class GraphNode<Item> {

    /**
     *
     * Task status
     */
    public enum TaskStatus {
        BLOCKING, // On the dag graph, but there are still have dependent parent nodes
        WAITING, // On the dag graph, have not dependent parent nodes, waited to be scheduled
        RUNNING, // Running
        DONE // Already Running done
    }

    private volatile TaskStatus        taskStatus;

    //A set of nodes that depend on this node
    private final Set<GraphNode<Item>> childSet;

    //A set of nodes this node depends of
    private final Set<GraphNode<Item>> parentSet;

    //Task
    private final Item                 item;

    public GraphNode(final Item item) {
        this.childSet = new HashSet<>();
        this.parentSet = new HashSet<>();
        this.item = item;
        this.taskStatus = TaskStatus.WAITING;
    }

    public void addParent(final GraphNode<Item> parentNode) {
        this.parentSet.add(parentNode);
    }

    public void removeParent(final GraphNode<Item> parentNode) {
        if (this.parentSet.contains(parentNode)) {
            this.parentSet.remove(parentNode);
            if (this.parentSet.isEmpty()) {
                this.taskStatus = TaskStatus.WAITING;
            }
        }
    }

    public void notifyChildren() {
        for (final GraphNode<Item> child : this.childSet) {
            child.removeParent(this);
        }
    }

    public void addChild(final GraphNode<Item> graphNode) {
        this.childSet.add(graphNode);
    }

    public void setTaskStatus(final TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    public boolean isWaiting() {
        return this.taskStatus == TaskStatus.WAITING;
    }

    public boolean isRunning() {
        return this.taskStatus == TaskStatus.RUNNING;
    }

    public boolean isDone() {
        return this.taskStatus == TaskStatus.DONE;
    }

    public Item getItem() {
        return this.item;
    }

    @Override
    public String toString() {
        return "GraphNode{" + "taskStatus=" + taskStatus + ", item=" + item + '}';
    }
}
