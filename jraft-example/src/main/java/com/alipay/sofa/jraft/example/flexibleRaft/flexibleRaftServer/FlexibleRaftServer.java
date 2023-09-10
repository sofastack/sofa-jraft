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
package com.alipay.sofa.jraft.example.flexibleRaft.flexibleRaftServer;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.example.flexibleRaft.FlexibleRaftService;
import com.alipay.sofa.jraft.example.flexibleRaft.FlexibleRaftServiceImpl;
import com.alipay.sofa.jraft.example.flexibleRaft.FlexibleStateMachine;
import com.alipay.sofa.jraft.example.flexibleRaft.rpc.FlexibleGetValueRequestProcessor;
import com.alipay.sofa.jraft.example.flexibleRaft.rpc.FlexibleGrpcHelper;
import com.alipay.sofa.jraft.example.flexibleRaft.rpc.FlexibleIncrementAndGetRequestProcessor;
import com.alipay.sofa.jraft.example.flexibleRaft.rpc.FlexibleRaftOutter;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author Akai
 */
public class FlexibleRaftServer {
    private RaftGroupService     raftGroupService;
    private Node                 node;
    private FlexibleStateMachine fsm;

    public FlexibleRaftServer(final String dataPath, final String groupId, final PeerId serverId,
                              final NodeOptions nodeOptions) throws IOException {
        // init raft data path, it contains log,meta,snapshot
        FileUtils.forceMkdir(new File(dataPath));

        // here use same RPC server for raft and business. It also can be seperated generally
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        // GrpcServer need init marshaller
        FlexibleGrpcHelper.initGRpc();
        FlexibleGrpcHelper.setRpcServer(rpcServer);

        // register business processor
        FlexibleRaftService flexibleRaftService = new FlexibleRaftServiceImpl(this);
        rpcServer.registerProcessor(new FlexibleGetValueRequestProcessor(flexibleRaftService));
        rpcServer.registerProcessor(new FlexibleIncrementAndGetRequestProcessor(flexibleRaftService));
        // init state machine
        this.fsm = new FlexibleStateMachine();
        // set fsm to nodeOptions
        nodeOptions.setFsm(this.fsm);
        // set storage path (log,meta,snapshot)
        // log, must
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // meta, must
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        // snapshot, optional, generally recommended
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // n=5 w=2,r=4
        nodeOptions.enableFlexibleRaft(true);
        nodeOptions.setFactor(6, 4);
        // init raft group service framework
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer);
        // start raft node
        this.node = this.raftGroupService.start();
    }

    public FlexibleStateMachine getFsm() {
        return this.fsm;
    }

    public Node getNode() {
        return this.node;
    }

    public RaftGroupService RaftGroupService() {
        return this.raftGroupService;
    }

    /**
     * Redirect request to new leader
     */
    public FlexibleRaftOutter.FlexibleValueResponse redirect() {
        final FlexibleRaftOutter.FlexibleValueResponse.Builder builder = FlexibleRaftOutter.FlexibleValueResponse
            .newBuilder().setSuccess(false);
        if (this.node != null) {
            final PeerId leader = this.node.getLeaderId();
            if (leader != null) {
                builder.setRedirect(leader.toString());
            }
        }
        return builder.build();
    }

    public static void main(final String[] args) throws IOException {
        if (args.length != 4) {
            System.out
                .println("Usage : java com.alipay.sofa.jraft.example.counter.CounterServer {dataPath} {groupId} {serverId} {initConf}");
            System.out
                .println("Example: java com.alipay.sofa.jraft.example.counter.CounterServer /tmp/server1 counter 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }
        final String dataPath = args[0];
        final String groupId = args[1];
        final String serverIdStr = args[2];
        final String initConfStr = args[3];

        final NodeOptions nodeOptions = new NodeOptions();
        // for test, modify some params
        // set election timeout to 1s
        nodeOptions.setElectionTimeoutMs(1000);
        // disable CLI service。
        nodeOptions.setDisableCli(false);
        // do snapshot every 30s
        nodeOptions.setSnapshotIntervalSecs(30);
        // parse server address
        final PeerId serverId = new PeerId();
        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }
        final Configuration initConf = new Configuration();

        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        // set cluster configuration
        nodeOptions.setInitialConf(initConf);

        // start raft server
        final FlexibleRaftServer flexibleRaftServer = new FlexibleRaftServer(dataPath, groupId, serverId, nodeOptions);
        System.out.println("Started counter server at port:"
                           + flexibleRaftServer.getNode().getNodeId().getPeerId().getPort());
        // GrpcServer need block to prevent process exit
        FlexibleGrpcHelper.blockUntilShutdown();
    }

}
