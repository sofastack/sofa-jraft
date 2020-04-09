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
package com.alipay.sofa.jraft.example.election;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;

/**
 *
 * @author jiachun.fjc
 */
public class ElectionNode implements Lifecycle<ElectionNodeOptions> {

    private static final Logger             LOG       = LoggerFactory.getLogger(ElectionNode.class);

    private final List<LeaderStateListener> listeners = new CopyOnWriteArrayList<>();
    private RaftGroupService                raftGroupService;
    private Node                            node;
    private ElectionOnlyStateMachine        fsm;

    private boolean                         started;

    @Override
    public boolean init(final ElectionNodeOptions opts) {
        if (this.started) {
            LOG.info("[ElectionNode: {}] already started.", opts.getServerAddress());
            return true;
        }
        // node options
        NodeOptions nodeOpts = opts.getNodeOptions();
        if (nodeOpts == null) {
            nodeOpts = new NodeOptions();
        }
        this.fsm = new ElectionOnlyStateMachine(this.listeners);
        nodeOpts.setFsm(this.fsm);
        final Configuration initialConf = new Configuration();
        if (!initialConf.parse(opts.getInitialServerAddressList())) {
            throw new IllegalArgumentException("Fail to parse initConf: " + opts.getInitialServerAddressList());
        }
        // Set the initial cluster configuration
        nodeOpts.setInitialConf(initialConf);
        final String dataPath = opts.getDataPath();
        try {
            FileUtils.forceMkdir(new File(dataPath));
        } catch (final IOException e) {
            LOG.error("Fail to make dir for dataPath {}.", dataPath);
            return false;
        }
        // Set the data path
        // Log, required
        nodeOpts.setLogUri(Paths.get(dataPath, "log").toString());
        // Metadata, required
        nodeOpts.setRaftMetaUri(Paths.get(dataPath, "meta").toString());
        // nodeOpts.setSnapshotUri(Paths.get(dataPath, "snapshot").toString());

        final String groupId = opts.getGroupId();
        final PeerId serverId = new PeerId();
        if (!serverId.parse(opts.getServerAddress())) {
            throw new IllegalArgumentException("Fail to parse serverId: " + opts.getServerAddress());
        }
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOpts, rpcServer);
        this.node = this.raftGroupService.start();
        if (this.node != null) {
            this.started = true;
        }
        return this.started;
    }

    @Override
    public void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                ThrowUtil.throwException(e);
            }
        }
        this.started = false;
        LOG.info("[ElectionNode] shutdown successfully: {}.", this);
    }

    public Node getNode() {
        return node;
    }

    public ElectionOnlyStateMachine getFsm() {
        return fsm;
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isLeader() {
        return this.fsm.isLeader();
    }

    public void addLeaderStateListener(final LeaderStateListener listener) {
        this.listeners.add(listener);
    }
}
