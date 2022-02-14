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
package com.alipay.sofa.jraft.example.counter;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.example.counter.rpc.CounterOutter.ValueResponse;
import com.alipay.sofa.jraft.example.counter.rpc.GetValueRequestProcessor;
import com.alipay.sofa.jraft.example.counter.rpc.CounterGrpcHelper;
import com.alipay.sofa.jraft.example.counter.rpc.IncrementAndGetRequestProcessor;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Counter server that keeps a counter value in a raft group.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * <p>
 * 2018-Apr-09 4:51:02 PM
 */
public class CounterServer {

    private RaftGroupService    raftGroupService;
    private Node                node;
    private CounterStateMachine fsm;

    public CounterServer(final String dataPath, final String groupId, final PeerId serverId,
                         final NodeOptions nodeOptions) throws IOException {
        // init raft data path, it contains log,meta,snapshot
        FileUtils.forceMkdir(new File(dataPath));

        // here use same RPC server for raft and business. It also can be seperated generally
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        // GrpcServer need init marshaller
        CounterGrpcHelper.initGRpc();
        CounterGrpcHelper.setRpcServer(rpcServer);

        // register business processor
        CounterService counterService = new CounterServiceImpl(this);
        rpcServer.registerProcessor(new GetValueRequestProcessor(counterService));
        rpcServer.registerProcessor(new IncrementAndGetRequestProcessor(counterService));
        // init state machine
        this.fsm = new CounterStateMachine();
        // set fsm to nodeOptions
        nodeOptions.setFsm(this.fsm);
        // set storage path (log,meta,snapshot)
        // log, must
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // meta, must
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        // snapshot, optional, generally recommended
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // init raft group service framework
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer);
        // start raft node
        this.node = this.raftGroupService.start();
    }

    public CounterStateMachine getFsm() {
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
    public ValueResponse redirect() {
        final ValueResponse.Builder builder = ValueResponse.newBuilder().setSuccess(false);
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
        final CounterServer counterServer = new CounterServer(dataPath, groupId, serverId, nodeOptions);
        System.out.println("Started counter server at port:"
                           + counterServer.getNode().getNodeId().getPeerId().getPort());
        // GrpcServer need block to prevent process exit
        CounterGrpcHelper.blockUntilShutdown();
    }
}
