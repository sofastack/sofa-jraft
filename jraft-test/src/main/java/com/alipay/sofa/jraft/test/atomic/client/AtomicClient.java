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
package com.alipay.sofa.jraft.test.atomic.client;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory;
import com.alipay.sofa.jraft.rpc.impl.MarshallerRegistry;
import com.alipay.sofa.jraft.test.atomic.HashUtils;
import com.alipay.sofa.jraft.test.atomic.KeyNotFoundException;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.IncrementAndGetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.SetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetSlotsCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.CompareAndSetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseResponseCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

/**
 * A counter client
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-25 4:08:14 PM
 */
public class AtomicClient {

    static final Logger                LOG    = LoggerFactory.getLogger(AtomicClient.class);

    private final Configuration        conf;
    private final CliClientServiceImpl cliClientService;
    private RpcClient                  rpcClient;
    private CliOptions                 cliOptions;
    private Map<Long, String>          groups = new TreeMap<>();

    public AtomicClient(String groupId, Configuration conf) {
        super();
        this.conf = conf;
        this.cliClientService = new CliClientServiceImpl();
    }

    public void shutdown() {
        this.cliClientService.shutdown();
        for (final String groupId : groups.values()) {
            RouteTable.getInstance().removeGroup(groupId);
        }
    }

    public void start() throws InterruptedException, TimeoutException {

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

        cliOptions = new CliOptions();
        this.cliClientService.init(cliOptions);
        this.rpcClient = this.cliClientService.getRpcClient();
        if (conf != null) {
            final Set<PeerId> peers = conf.getPeerSet();
            for (final PeerId peer : peers) {
                try {
                    final GetSlotsCommand getSlotsCommand = GetSlotsCommand.newBuilder().build();
                    final BaseResponseCommand cmd = (BaseResponseCommand) this.rpcClient.invokeSync(peer.getEndpoint(),
                        getSlotsCommand, cliOptions.getRpcDefaultTimeout());
                    groups = cmd.getMapMap();
                    if (groups.size() > 0) {
                        break;
                    } else {
                        LOG.warn("Fail to get slots from peer {}, error: {}", peer, cmd.getErrorMsg());
                    }
                } catch (final Throwable t) {
                    LOG.warn("Fail to get slots from peer {}, error: {}", peer, t.getMessage());
                    //continue;
                }
            }

            if (groups == null || groups.isEmpty()) {
                throw new IllegalArgumentException("Can't get slots from any peers");
            } else {
                LOG.info("All groups  is {}", groups);
            }
            for (final String groupId : groups.values()) {
                RouteTable.getInstance().updateConfiguration(groupId, conf);
                refreshLeader(groupId);
                refreshConf(groupId);
            }
        }

    }

    private void refreshConf(String groupId) throws InterruptedException, TimeoutException {
        RouteTable.getInstance().refreshConfiguration(cliClientService, groupId, cliOptions.getRpcDefaultTimeout());
    }

    private void refreshLeader(String groupId) throws InterruptedException, TimeoutException {
        RouteTable.getInstance().refreshLeader(cliClientService, groupId, cliOptions.getRpcDefaultTimeout());
    }

    private String getGroupId(String key) {
        return groups.get(HashUtils.getHeadKey(groups, key));
    }

    public long get(String key) throws InterruptedException, KeyNotFoundException, TimeoutException {
        return this.get(getLeaderByKey(key), key, false, false);
    }

    private PeerId getLeaderByKey(String key) throws InterruptedException, TimeoutException {
        return getLeader(getGroupId(key));
    }

    private PeerId getLeader(String key) throws InterruptedException, TimeoutException {
        final String groupId = getGroupId(key);
        refreshLeader(groupId);
        return RouteTable.getInstance().selectLeader(groupId);
    }

    private PeerId getPeer(String key) throws InterruptedException, TimeoutException {
        final String groupId = getGroupId(key);
        this.refreshConf(groupId);
        final List<PeerId> peers = RouteTable.getInstance().getConfiguration(groupId).getPeers();
        return peers.get(ThreadLocalRandom.current().nextInt(peers.size()));
    }

    public long get(String key, boolean readFromQuorum) throws KeyNotFoundException, InterruptedException,
                                                       TimeoutException {
        if (readFromQuorum) {
            return get(getPeer(key), key, true, false);
        } else {
            //read from leader
            return get(key);
        }
    }

    public long get(PeerId peer, String key, boolean readFromQuorum, boolean readByStateMachine) {
        try {
            // Firstly, build a getCmd object instance.
            final GetCommand getCmd = GetCommand.newBuilder().setReadByStateMachine(readByStateMachine)
                .setReadFromQuorum(readFromQuorum).build();

            // Then, build a baseReqCmd object instance.
            final BaseRequestCommand baseReqCmd = BaseRequestCommand.newBuilder()
                .setRequestType(BaseRequestCommand.RequestType.get).setKey(key).setExtension(GetCommand.body, getCmd)
                .build();

            final Object response = this.rpcClient.invokeSync(peer.getEndpoint(), baseReqCmd,
                cliOptions.getRpcDefaultTimeout());
            final BaseResponseCommand cmd = (BaseResponseCommand) response;
            if (cmd.getSuccess()) {
                return ((BaseResponseCommand) cmd).getVlaue();
            } else {
                if (cmd.getErrorMsg().equals("key not found")) {
                    throw new KeyNotFoundException();
                } else {
                    throw new IllegalStateException("Server error:" + cmd.getErrorMsg());
                }
            }
        } catch (final Throwable t) {
            t.printStackTrace();
            throw new IllegalStateException("Remoting error:" + t.getMessage());
        }
    }

    public long addAndGet(String key, long delta) throws InterruptedException, TimeoutException {
        return this.addAndGet(getLeaderByKey(key), key, delta);
    }

    public long addAndGet(PeerId peer, String key, long delta) {
        try {

            // Firstly, build a inAndGetCmd object instance.
            final IncrementAndGetCommand inAndGetCmd = IncrementAndGetCommand.newBuilder().setDetal(delta).build();

            // Then, build a baseReqCmd object instance.
            final BaseRequestCommand baseReqCmd = BaseRequestCommand.newBuilder()
                .setRequestType(BaseRequestCommand.RequestType.incrementAndGet).setKey(key)
                .setExtension(IncrementAndGetCommand.body, inAndGetCmd).build();

            final Object response = this.rpcClient.invokeSync(peer.getEndpoint(), baseReqCmd,
                cliOptions.getRpcDefaultTimeout());
            final BaseResponseCommand cmd = (BaseResponseCommand) response;
            if (cmd.getSuccess()) {
                return ((BaseResponseCommand) cmd).getVlaue();
            } else {
                throw new IllegalStateException("Server error:" + cmd.getErrorMsg());
            }
        } catch (final Throwable t) {
            throw new IllegalStateException("Remoting error:" + t.getMessage());
        }
    }

    public boolean set(String key, long value) throws InterruptedException, TimeoutException {
        return this.set(getLeaderByKey(key), key, value);
    }

    public boolean set(PeerId peer, String key, long value) {
        try {
            // Firstly, build a setCmd object instance.
            final SetCommand setCmd = SetCommand.newBuilder().setValue(value).build();

            // Then, build a baseReqCmd object instance.
            final BaseRequestCommand baseReqCmd = BaseRequestCommand.newBuilder()
                .setRequestType(BaseRequestCommand.RequestType.set).setKey(key).setExtension(SetCommand.body, setCmd)
                .build();

            final Object response = this.rpcClient.invokeSync(peer.getEndpoint(), baseReqCmd,
                cliOptions.getRpcDefaultTimeout());
            final BaseResponseCommand cmd = (BaseResponseCommand) response;
            return cmd.getSuccess();
        } catch (final Throwable t) {
            throw new IllegalStateException("Remoting error:" + t.getMessage());
        }
    }

    public boolean compareAndSet(String key, long expect, long newVal) throws InterruptedException, TimeoutException {
        return this.compareAndSet(getLeaderByKey(key), key, expect, newVal);
    }

    public boolean compareAndSet(PeerId peer, String key, long expect, long newVal) {
        try {

            // Firstly, build a cmpAndSetCmd object instance.
            final CompareAndSetCommand cmpAndSetCmd = CompareAndSetCommand.newBuilder().setExpect(expect)
                .setNewValue(newVal).build();

            // Then, build a baseReqCmd object instance.
            final BaseRequestCommand baseReqCmd = BaseRequestCommand.newBuilder()
                .setRequestType(BaseRequestCommand.RequestType.compareAndSet).setKey(key)
                .setExtension(CompareAndSetCommand.body, cmpAndSetCmd).build();

            final Object response = this.rpcClient.invokeSync(peer.getEndpoint(), baseReqCmd,
                cliOptions.getRpcDefaultTimeout());
            final BaseResponseCommand cmd = (BaseResponseCommand) response;
            return cmd.getSuccess();
        } catch (final Throwable t) {
            throw new IllegalStateException("Remoting error:" + t.getMessage());
        }
    }

}
