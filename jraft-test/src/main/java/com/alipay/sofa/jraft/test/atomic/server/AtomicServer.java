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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.test.atomic.HashUtils;
import com.alipay.sofa.jraft.test.atomic.server.processor.CompareAndSetCommandProcessor;
import com.alipay.sofa.jraft.test.atomic.server.processor.GetCommandProcessor;
import com.alipay.sofa.jraft.test.atomic.server.processor.GetSlotsCommandProcessor;
import com.alipay.sofa.jraft.test.atomic.server.processor.IncrementAndGetCommandProcessor;
import com.alipay.sofa.jraft.test.atomic.server.processor.SetCommandProcessor;

/**
 * Atomic server with multi raft groups.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-May-02 10:50:14 AM
 */
public class AtomicServer {

    private static final Logger            LOG    = LoggerFactory.getLogger(AtomicServer.class);

    public TreeMap<Long, AtomicRangeGroup> nodes  = new TreeMap<>();
    public TreeMap<Long, String>           groups = new TreeMap<>();
    private int                            totalSlots;
    private StartupConf                    conf;

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

    //for test

    public static void main(String[] arsg) throws Exception {
        StartupConf conf = new StartupConf();
        conf.start("config/server.properties");
    }
}
