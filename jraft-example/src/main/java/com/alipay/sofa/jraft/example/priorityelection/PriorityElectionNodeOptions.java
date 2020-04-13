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
package com.alipay.sofa.jraft.example.priorityelection;

import com.alipay.sofa.jraft.option.NodeOptions;

/**
 * @author zongtanghu
 */
public class PriorityElectionNodeOptions {

    private String      dataPath;
    // raft group id
    private String      groupId;
    // ip:port::priority
    private String      serverAddress;
    // ip:port::priority,ip:port::priority,ip:port::priority
    private String      initialServerAddressList;
    // raft node options
    private NodeOptions nodeOptions;

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public String getInitialServerAddressList() {
        return initialServerAddressList;
    }

    public void setInitialServerAddressList(String initialServerAddressList) {
        this.initialServerAddressList = initialServerAddressList;
    }

    public NodeOptions getNodeOptions() {
        return nodeOptions;
    }

    public void setNodeOptions(NodeOptions nodeOptions) {
        this.nodeOptions = nodeOptions;
    }

    @Override
    public String toString() {
        return "PriorityElectionNodeOptions{" + "dataPath='" + dataPath + '\'' + ", groupId='" + groupId + '\''
               + ", serverAddress='" + serverAddress + '\'' + ", initialServerAddressList='" + initialServerAddressList
               + '\'' + ", nodeOptions=" + nodeOptions + '}';
    }
}
