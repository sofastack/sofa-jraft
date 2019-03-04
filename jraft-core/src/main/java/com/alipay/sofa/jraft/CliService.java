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
package com.alipay.sofa.jraft;

import java.util.List;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;

/**
 * Client command-line service
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 4:05:35 PM
 */
public interface CliService extends Lifecycle<CliOptions> {

    /**
     * Add a new peer into the replicating group which consists of |conf|.
     * return OK status when success.
     *
     * @param groupId the raft group id
     * @param conf    current configuration
     * @param peer    peer to add
     * @return operation status
     */
    Status addPeer(String groupId, Configuration conf, PeerId peer);

    /**
     * Remove a peer from the replicating group which consists of |conf|.
     * return OK status when success.
     *
     * @param groupId the raft group id
     * @param conf    current configuration
     * @param peer    peer to remove
     * @return operation status
     */
    Status removePeer(String groupId, Configuration conf, PeerId peer);

    /**
     * Gracefully change the peers of the replication group.
     *
     * @param groupId  the raft group id
     * @param conf     current configuration
     * @param newPeers new peers to change
     * @return operation status
     */
    Status changePeers(String groupId, Configuration conf, Configuration newPeers);

    /**
     * Reset the peer set of the target peer.
     *
     * @param groupId  the raft group id
     * @param peer     dest
     * @param newPeers new peers to reset
     * @return operation status
     */
    Status resetPeer(String groupId, PeerId peer, Configuration newPeers);

    /**
     * Transfer the leader of the replication group to the target peer
     *
     * @param groupId the raft group id
     * @param conf    current configuration
     * @param peer    target peer of new leader
     * @return operation status
     */
    Status transferLeader(String groupId, Configuration conf, PeerId peer);

    /**
     * Ask the peer to dump a snapshot immediately.
     *
     * @param groupId the raft group id
     * @param peer    target peer
     * @return operation status
     */
    Status snapshot(String groupId, PeerId peer);

    /**
     * Get the leader of the replication group.
     * @param groupId  the raft group id
     * @param conf     configuration
     * @param leaderId id of leader
     * @return status
     */
    Status getLeader(String groupId, Configuration conf, PeerId leaderId);

    /**
     * Ask all peers of of the replication group.
     *
     * @param groupId the raft group id
     * @param conf    target peers configuration
     * @return operation status
     */
    List<PeerId> getPeers(String groupId, Configuration conf);
}
