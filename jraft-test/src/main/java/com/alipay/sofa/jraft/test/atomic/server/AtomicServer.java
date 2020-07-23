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
package com.alipay.sofa.jraft.test.atomic.server;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory;
import com.alipay.sofa.jraft.rpc.impl.MarshallerRegistry;
import com.alipay.sofa.jraft.test.atomic.server.processor.DefaultKVService;
import com.alipay.sofa.jraft.test.atomic.server.processor.KVCommandProcessor;
import com.alipay.sofa.jraft.test.atomic.server.processor.KVService;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.IncrementAndGetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.SetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetSlotsCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.CompareAndSetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseResponseCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.test.atomic.HashUtils;

/**
 * Atomic server with multi raft groups.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-May-02 10:50:14 AM
 */
public class AtomicServer {

    private static final Logger             LOG    = LoggerFactory.getLogger(AtomicServer.class);

    private TreeMap<Long, AtomicRangeGroup> nodes  = new TreeMap<>();
    private TreeMap<Long, String>           groups = new TreeMap<>();
    private int                             totalSlots;
    private StartupConf                     conf;
    private KVService                       kvService;

    public AtomicRangeGroup getGroupBykey(String key) {
        return nodes.get(HashUtils.getHeadKey(this.nodes, key));
    }

    public int getTotalSlots() {
        return this.totalSlots;
    }

    public AtomicServer(StartupConf conf) throws IOException {
        this.conf = conf;
        LOG.info("Starting atomic server with conf: {}", this.conf);
        this.totalSlots = conf.getTotalSlots();
    }

    public TreeMap<Long, String> getGroups() {
        return this.groups;
    }

    public void start() throws IOException {
        PeerId serverId = new PeerId();
        if (!serverId.parse(conf.getServerAddress())) {
            throw new IllegalArgumentException("Fail to parse serverId:" + conf.getServerAddress());
        }

        FileUtils.forceMkdir(new File(conf.getDataPath()));

        // The same in-process raft group shares the same RPC Server.
        GrpcRaftRpcFactory raftRpcFactory = (GrpcRaftRpcFactory) RpcFactoryHelper.rpcFactory();
        // Register request and response proto.
        raftRpcFactory.registerProtobufSerializer(GetCommand.class.getName(), GetCommand.getDefaultInstance());
        raftRpcFactory
            .registerProtobufSerializer(GetSlotsCommand.class.getName(), GetSlotsCommand.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(IncrementAndGetCommand.class.getName(),
            IncrementAndGetCommand.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(CompareAndSetCommand.class.getName(),
            CompareAndSetCommand.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(SetCommand.class.getName(), SetCommand.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(BaseResponseCommand.class.getName(),
            BaseResponseCommand.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(BaseRequestCommand.class.getName(),
            BaseRequestCommand.getDefaultInstance());

        // Register request and response relationship.
        MarshallerRegistry registry = raftRpcFactory.getMarshallerRegistry();
        registry.registerResponseInstance(GetSlotsCommand.class.getName(), BaseResponseCommand.getDefaultInstance());
        registry.registerResponseInstance(GetCommand.class.getName(), BaseResponseCommand.getDefaultInstance());
        registry.registerResponseInstance(IncrementAndGetCommand.class.getName(),
            BaseResponseCommand.getDefaultInstance());
        registry.registerResponseInstance(CompareAndSetCommand.class.getName(),
            BaseResponseCommand.getDefaultInstance());
        registry.registerResponseInstance(SetCommand.class.getName(), BaseResponseCommand.getDefaultInstance());

        // The same in-process raft group shares the same RPC Server.
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        // Register biz handler
        this.kvService = new DefaultKVService(this);
        rpcServer.registerProcessor(new KVCommandProcessor(this.kvService));

        long step = conf.getMaxSlot() / totalSlots;
        if (conf.getMaxSlot() % totalSlots > 0) {
            step = step + 1;
        }
        for (int i = 0; i < totalSlots; i++) {
            long min = i * step;
            long mayMax = (i + 1) * step;
            long max = mayMax > conf.getMaxSlot() || mayMax <= 0 ? conf.getMaxSlot() : mayMax;
            StartupConf nodeConf = new StartupConf();
            String nodeDataPath = conf.getDataPath() + File.separator + i;
            nodeConf.setDataPath(nodeDataPath);
            String nodeGroup = conf.getGroupId() + "_" + i;
            nodeConf.setGroupId(nodeGroup);
            nodeConf.setMaxSlot(max);
            nodeConf.setMinSlot(min);
            nodeConf.setConf(conf.getConf());
            nodeConf.setServerAddress(conf.getServerAddress());
            nodeConf.setTotalSlots(conf.getTotalSlots());
            LOG.info("Starting range node {}-{} with conf {}", min, max, nodeConf);
            nodes.put(i * step, AtomicRangeGroup.start(nodeConf, rpcServer));
            groups.put(i * step, nodeGroup);
        }
    }

    public static void start(String confFilePath) throws IOException {
        StartupConf conf = new StartupConf();
        if (!conf.loadFromFile(confFilePath)) {
            throw new IllegalStateException("Load startup config from " + confFilePath + " failed");
        }
        AtomicServer server = new AtomicServer(conf);
        server.start();
    }

    //for test
    public static void main(String[] arsg) throws Exception {
        start("config/server.properties");
    }
}
