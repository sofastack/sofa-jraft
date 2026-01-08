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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.test.atomic.server.processor.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.conf.Configuration;

import static com.alipay.sofa.jraft.test.atomic.server.AtomicRangeGroup.LOG;

public class StartupConf {

    public StartupConf() {

    }

    private String groupId;
    private String dataPath;
    private String conf;
    private String serverAddress;
    private long   minSlot;
    private long   maxSlot;
    private int    totalSlots = 1;

    public int getTotalSlots() {
        return this.totalSlots;
    }

    public void setTotalSlots(int totalSlots) {
        this.totalSlots = totalSlots;
    }

    public long getMinSlot() {
        return this.minSlot;
    }

    public void setMinSlot(long minSlot) {
        this.minSlot = minSlot;
    }

    public long getMaxSlot() {
        return this.maxSlot;
    }

    public void setMaxSlot(long maxSlot) {
        this.maxSlot = maxSlot;
    }

    public boolean loadFromFile(String file) throws IOException {
        try (FileInputStream fin = new FileInputStream(new File(file))) {
            Properties props = new Properties();
            props.load(fin);
            this.groupId = props.getProperty("groupId");
            this.dataPath = props.getProperty("dataPath", "/tmp/atomic");
            this.conf = props.getProperty("conf");
            this.serverAddress = props.getProperty("serverAddress");
            this.minSlot = Long.valueOf(props.getProperty("minSlot", "0"));
            this.maxSlot = Long.valueOf(props.getProperty("maxSlot", String.valueOf(Long.MAX_VALUE)));
            this.totalSlots = Integer.valueOf(props.getProperty("totalSlots", "1"));
            return this.verify();
        }
    }

    private boolean verify() {
        if (StringUtils.isBlank(groupId)) {
            throw new IllegalArgumentException("Blank groupId");
        }
        if (StringUtils.isBlank(dataPath)) {
            throw new IllegalArgumentException("Blank dataPath");
        }
        if (StringUtils.isBlank(conf)) {
            throw new IllegalArgumentException("Blank conf");
        }
        Configuration initConf = JRaftUtils.getConfiguration(conf);
        if (initConf.isEmpty()) {
            throw new IllegalArgumentException("Blank conf");
        }
        if (minSlot < 0) {
            throw new IllegalArgumentException("Invalid min slot");
        }
        if (minSlot > maxSlot) {
            throw new IllegalArgumentException("Invalid slot range.");
        }
        if (this.totalSlots <= 0) {
            throw new IllegalArgumentException("Invalid total slots");
        }
        return true;
    }

    public String getGroupId() {
        return this.groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getDataPath() {
        return this.dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getConf() {
        return this.conf;
    }

    public void setConf(String conf) {
        this.conf = conf;
    }

    public String getServerAddress() {
        return this.serverAddress;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public String toString() {
        return "StartupConf [groupId=" + this.groupId + ", dataPath=" + this.dataPath + ", conf=" + this.conf
               + ", serverAddress=" + this.serverAddress + ", minSlot=" + this.minSlot + ", maxSlot=" + this.maxSlot
               + ", totalSlots=" + this.totalSlots + "]";
    }

    public void start() throws IOException {
        //        StartupConf conf = new StartupConf();
        AtomicServer server = new AtomicServer(this);
        PeerId serverId = new PeerId();
        if (!serverId.parse(this.getServerAddress())) {
            throw new IllegalArgumentException("Fail to parse serverId:" + this.getServerAddress());
        }

        FileUtils.forceMkdir(new File(this.getDataPath()));
        // The same in-process raft group shares the same RPC Server.
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        // Register biz handler
        rpcServer.registerProcessor(new GetSlotsCommandProcessor(server));
        rpcServer.registerProcessor(new GetCommandProcessor(server));
        rpcServer.registerProcessor(new IncrementAndGetCommandProcessor(server));
        rpcServer.registerProcessor(new CompareAndSetCommandProcessor(server));
        rpcServer.registerProcessor(new SetCommandProcessor(server));

        long step = this.getMaxSlot() / totalSlots;
        if (this.getMaxSlot() % totalSlots > 0) {
            step = step + 1;
        }
        for (int i = 0; i < totalSlots; i++) {
            long min = i * step;
            long mayMax = (i + 1) * step;
            long max = mayMax > this.getMaxSlot() || mayMax <= 0 ? this.getMaxSlot() : mayMax;
            StartupConf nodeConf = new StartupConf();
            String nodeDataPath = this.getDataPath() + File.separator + i;
            nodeConf.setDataPath(nodeDataPath);
            String nodeGroup = this.getGroupId() + "_" + i;
            nodeConf.setGroupId(nodeGroup);
            nodeConf.setMaxSlot(max);
            nodeConf.setMinSlot(min);
            nodeConf.setConf(this.getConf());
            nodeConf.setServerAddress(this.getServerAddress());
            nodeConf.setTotalSlots(this.getTotalSlots());
            LOG.info("Starting range node {}-{} with conf {}", min, max, nodeConf);
            server.nodes.put(i * step, AtomicRangeGroup.start(nodeConf, rpcServer));
            server.groups.put(i * step, nodeGroup);
        }
    }

    public void start(String confFilePath) throws IOException {
        StartupConf conf = new StartupConf();
        if (!conf.loadFromFile(confFilePath)) {
            throw new IllegalStateException("Load startup config from " + confFilePath + " failed");
        }
        this.start();
    }

}
