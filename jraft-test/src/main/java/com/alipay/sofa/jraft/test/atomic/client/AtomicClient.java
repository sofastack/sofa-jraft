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
import com.alipay.sofa.jraft.test.atomic.HashUtils;
import com.alipay.sofa.jraft.test.atomic.KeyNotFoundException;
import com.alipay.sofa.jraft.test.atomic.command.BooleanCommand;
import com.alipay.sofa.jraft.test.atomic.command.CompareAndSetCommand;
import com.alipay.sofa.jraft.test.atomic.command.GetCommand;
import com.alipay.sofa.jraft.test.atomic.command.GetSlotsCommand;
import com.alipay.sofa.jraft.test.atomic.command.IncrementAndGetCommand;
import com.alipay.sofa.jraft.test.atomic.command.SetCommand;
import com.alipay.sofa.jraft.test.atomic.command.SlotsResponseCommand;
import com.alipay.sofa.jraft.test.atomic.command.ValueCommand;

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
    private TreeMap<Long, String>      groups = new TreeMap<>();

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
        cliOptions = new CliOptions();
        this.cliClientService.init(cliOptions);
        this.rpcClient = this.cliClientService.getRpcClient();
        if (conf != null) {
            final Set<PeerId> peers = conf.getPeerSet();
            for (final PeerId peer : peers) {
                try {
                    final BooleanCommand cmd = (BooleanCommand) this.rpcClient.invokeSync(peer.getEndpoint(),
                        new GetSlotsCommand(), cliOptions.getRpcDefaultTimeout());
                    if (cmd instanceof SlotsResponseCommand) {
                        groups = ((SlotsResponseCommand) cmd).getMap();
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

    public long get(PeerId peer, String key, boolean readFromQuorum, boolean readByStateMachine)
                                                                                                throws KeyNotFoundException,
                                                                                                InterruptedException {
        try {
            final GetCommand request = new GetCommand(key);
            request.setReadFromQuorum(readFromQuorum);
            request.setReadByStateMachine(readByStateMachine);
            final Object response = this.rpcClient.invokeSync(peer.getEndpoint(), request,
                cliOptions.getRpcDefaultTimeout());
            final BooleanCommand cmd = (BooleanCommand) response;
            if (cmd.isSuccess()) {
                return ((ValueCommand) cmd).getVlaue();
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

    public long addAndGet(PeerId peer, String key, long delta) throws InterruptedException {
        try {
            final IncrementAndGetCommand request = new IncrementAndGetCommand();
            request.setKey(key);
            request.setDetal(delta);
            final Object response = this.rpcClient.invokeSync(peer.getEndpoint(), request,
                cliOptions.getRpcDefaultTimeout());
            final BooleanCommand cmd = (BooleanCommand) response;
            if (cmd.isSuccess()) {
                return ((ValueCommand) cmd).getVlaue();
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

    public boolean set(PeerId peer, String key, long value) throws InterruptedException {
        try {
            final SetCommand request = new SetCommand();
            request.setKey(key);
            request.setValue(value);
            final Object response = this.rpcClient.invokeSync(peer.getEndpoint(), request,
                cliOptions.getRpcDefaultTimeout());
            final BooleanCommand cmd = (BooleanCommand) response;
            return cmd.isSuccess();
        } catch (final Throwable t) {
            throw new IllegalStateException("Remoting error:" + t.getMessage());
        }
    }

    public boolean compareAndSet(String key, long expect, long newVal) throws InterruptedException, TimeoutException {
        return this.compareAndSet(getLeaderByKey(key), key, expect, newVal);
    }

    public boolean compareAndSet(PeerId peer, String key, long expect, long newVal) throws InterruptedException {
        try {
            final CompareAndSetCommand request = new CompareAndSetCommand();
            request.setKey(key);
            request.setNewValue(newVal);
            request.setExpect(expect);
            final Object response = this.rpcClient.invokeSync(peer.getEndpoint(), request,
                cliOptions.getRpcDefaultTimeout());
            final BooleanCommand cmd = (BooleanCommand) response;
            return cmd.isSuccess();
        } catch (final Throwable t) {
            throw new IllegalStateException("Remoting error:" + t.getMessage());
        }
    }

}
