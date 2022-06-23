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
package com.alipay.sofa.jraft.option;

import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.storage.LogStorage;

/**
 * Options for log manager.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-13 5:15:15 PM
 */
public class LogManagerOptions {
    private String               groupId;
    private LogStorage           logStorage;
    private ConfigurationManager configurationManager;
    private FSMCaller            fsmCaller;
    private int                  disruptorBufferSize  = 1024;
    private RaftOptions          raftOptions;
    private NodeMetrics          nodeMetrics;
    private LogEntryCodecFactory logEntryCodecFactory = LogEntryV2CodecFactory.getInstance();

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public LogEntryCodecFactory getLogEntryCodecFactory() {
        return this.logEntryCodecFactory;
    }

    public void setLogEntryCodecFactory(final LogEntryCodecFactory logEntryCodecFactory) {
        this.logEntryCodecFactory = logEntryCodecFactory;
    }

    public NodeMetrics getNodeMetrics() {
        return this.nodeMetrics;
    }

    public void setNodeMetrics(final NodeMetrics nodeMetrics) {
        this.nodeMetrics = nodeMetrics;
    }

    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    public void setRaftOptions(final RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

    public int getDisruptorBufferSize() {
        return this.disruptorBufferSize;
    }

    public void setDisruptorBufferSize(final int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
    }

    public LogStorage getLogStorage() {
        return this.logStorage;
    }

    public void setLogStorage(final LogStorage logStorage) {
        this.logStorage = logStorage;
    }

    public ConfigurationManager getConfigurationManager() {
        return this.configurationManager;
    }

    public void setConfigurationManager(final ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    public FSMCaller getFsmCaller() {
        return this.fsmCaller;
    }

    public void setFsmCaller(final FSMCaller fsmCaller) {
        this.fsmCaller = fsmCaller;
    }

}
