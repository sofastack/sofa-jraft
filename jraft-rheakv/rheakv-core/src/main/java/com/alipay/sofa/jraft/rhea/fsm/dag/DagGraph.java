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

import java.util.List;

/**
 * @author hzh (642256541@qq.com)
 */
public interface DagGraph<Item> {

    /**
     *
     * Add node
     */
    void addNode(final GraphNode<Item> node);

    /**
     *
     * Add node
     * @param childNode current node
     * @param parentNodes nodes that current node depend on
     */
    void addNode(final GraphNode<Item> childNode, final List<GraphNode<Item>> parentNodes);

    /**
     *
     * @return All nodes with input degree = 0
     */
    List<GraphNode<Item>> getReadyNodes();

    /**
     *
     * @return All nodes
     */
    List<GraphNode<Item>> getAllNodes();

    /**
     *
     * @param node notify that this node begins to run
     */
    void notifyStart(final GraphNode<Item> node);

    /**
     *
     * @param node notify that this node runs done
     */
    void notifyDone(final GraphNode<Item> node);
}
