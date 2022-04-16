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
package com.alipay.sofa.jraft.rpc.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.CustomSerializerManager;
import com.alipay.remoting.rpc.RpcConfigManager;
import com.alipay.remoting.rpc.RpcConfigs;
import com.alipay.sofa.jraft.rpc.ProtobufSerializer;
import com.alipay.sofa.jraft.rpc.RaftRpcFactory;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SPI;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;

/**
 *
 * @author jiachun.fjc
 */
@SPI
public class BoltRaftRpcFactory implements RaftRpcFactory {

    private static final Logger LOG                               = LoggerFactory.getLogger(BoltRaftRpcFactory.class);

    static final int            CHANNEL_WRITE_BUF_LOW_WATER_MARK  = SystemPropertyUtil.getInt(
                                                                      "bolt.channel_write_buf_low_water_mark",
                                                                      256 * 1024);
    static final int            CHANNEL_WRITE_BUF_HIGH_WATER_MARK = SystemPropertyUtil.getInt(
                                                                      "bolt.channel_write_buf_high_water_mark",
                                                                      512 * 1024);

    @Override
    public void registerProtobufSerializer(final String className, final Object... args) {
        CustomSerializerManager.registerCustomSerializer(className, ProtobufSerializer.INSTANCE);
    }

    @Override
    public RpcClient createRpcClient(final ConfigHelper<RpcClient> helper) {
        final com.alipay.remoting.rpc.RpcClient boltImpl = new com.alipay.remoting.rpc.RpcClient();
        final RpcClient rpcClient = new BoltRpcClient(boltImpl);
        if (helper != null) {
            helper.config(rpcClient);
        }
        return rpcClient;
    }

    @Override
    public RpcServer createRpcServer(final Endpoint endpoint, final ConfigHelper<RpcServer> helper) {
        final int port = Requires.requireNonNull(endpoint, "endpoint").getPort();
        Requires.requireTrue(port > 0 && port < 0xFFFF, "port out of range:" + port);
        final com.alipay.remoting.rpc.RpcServer boltImpl = new com.alipay.remoting.rpc.RpcServer(port, true, false);
        final RpcServer rpcServer = new BoltRpcServer(boltImpl);
        if (helper != null) {
            helper.config(rpcServer);
        }
        return rpcServer;
    }

    @Override
    public void ensurePipeline() {
        // enable `bolt.rpc.dispatch-msg-list-in-default-executor` system property
        if (RpcConfigManager.dispatch_msg_list_in_default_executor()) {
            System.setProperty(RpcConfigs.DISPATCH_MSG_LIST_IN_DEFAULT_EXECUTOR, "false");
            LOG.warn("JRaft SET {} to be false for replicator pipeline optimistic.",
                RpcConfigs.DISPATCH_MSG_LIST_IN_DEFAULT_EXECUTOR);
        }
    }
}
