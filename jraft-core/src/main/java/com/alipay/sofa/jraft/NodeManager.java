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
package com.alipay.sofa.jraft;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.ConcurrentHashSet;

/**
 * Raft nodes manager.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-22 5:58:23 PM
 */
public class NodeManager {

    private static final NodeManager                INSTANCE = new NodeManager();

    private final ConcurrentMap<NodeId, Node>       nodeMap  = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<Node>> groupMap = new ConcurrentHashMap<>();
    private final ConcurrentHashSet<Endpoint>       addrSet  = new ConcurrentHashSet<>();

    public static NodeManager getInstance() {
        return INSTANCE;
    }

    /**
     * Return true when RPC service is registered.
     */
    public boolean serverExists(final Endpoint addr) {
        if (addr.getIp().equals(Utils.IP_ANY)) {
            return this.addrSet.contains(new Endpoint(Utils.IP_ANY, addr.getPort()));
        }
        return this.addrSet.contains(addr);
    }

    /**
     * Remove a RPC service address.
     */
    public boolean removeAddress(final Endpoint addr) {
        return this.addrSet.remove(addr);
    }

    /**
     * Adds a RPC service address.
     */
    public void addAddress(final Endpoint addr) {
        this.addrSet.add(addr);
    }

    /**
     * Adds a node.
     */
    public boolean add(final Node node) {
        // check address ok?
        if (!serverExists(node.getNodeId().getPeerId().getEndpoint())) {
            return false;
        }
        final NodeId nodeId = node.getNodeId();
        if (this.nodeMap.putIfAbsent(nodeId, node) == null) {
            final String groupId = node.getGroupId();
            List<Node> nodes = this.groupMap.get(groupId);
            if (nodes == null) {
                nodes = Collections.synchronizedList(new ArrayList<>());
                List<Node> existsNode = this.groupMap.putIfAbsent(groupId, nodes);
                if (existsNode != null) {
                    nodes = existsNode;
                }
            }
            nodes.add(node);
            return true;
        }
        return false;
    }

    /**
     * Clear the states, for test
     */
    @OnlyForTest
    public void clear() {
        this.groupMap.clear();
        this.nodeMap.clear();
        this.addrSet.clear();
    }

    /**
     * Remove a node.
     */
    public boolean remove(final Node node) {
        if (this.nodeMap.remove(node.getNodeId(), node)) {
            final List<Node> nodes = this.groupMap.get(node.getGroupId());
            if (nodes != null) {
                return nodes.remove(node);
            }
        }
        return false;
    }

    /**
     * Get node by groupId and peer.
     */
    public Node get(final String groupId, final PeerId peerId) {
        return this.nodeMap.get(new NodeId(groupId, peerId));
    }

    /**
     * Get all nodes in a raft group.
     */
    public List<Node> getNodesByGroupId(final String groupId) {
        return this.groupMap.get(groupId);
    }

    /**
     * Get all nodes
     */
    public List<Node> getAllNodes() {
        return this.groupMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    private NodeManager() {
    }
}
