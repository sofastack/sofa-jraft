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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.StampedLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliClientService;
import com.alipay.sofa.jraft.rpc.CliRequests;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.Requires;
import com.google.protobuf.Message;

/**
 * Maintain routes to raft groups.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 10:41:21 AM
 */
public class RouteTable implements Describer {

    private static final Logger                    LOG            = LoggerFactory.getLogger(RouteTable.class);

    private static final RouteTable                INSTANCE       = new RouteTable();

    // Map<groupId, groupConf>
    private final ConcurrentMap<String, GroupConf> groupConfTable = new ConcurrentHashMap<>();

    public static RouteTable getInstance() {
        return INSTANCE;
    }

    /**
     * Update configuration of group in route table.
     *
     * @param groupId raft group id
     * @param conf    configuration to update
     * @return true on success
     */
    public boolean updateConfiguration(final String groupId, final Configuration conf) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");

        final GroupConf gc = getOrCreateGroupConf(groupId);
        final StampedLock stampedLock = gc.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            gc.conf = conf;
            if (gc.leader != null && !gc.conf.contains(gc.leader)) {
                gc.leader = null;
            }
        } finally {
            stampedLock.unlockWrite(stamp);
        }
        return true;
    }

    private GroupConf getOrCreateGroupConf(final String groupId) {
        GroupConf gc = this.groupConfTable.get(groupId);
        if (gc == null) {
            return this.groupConfTable.computeIfAbsent(groupId, key -> new GroupConf());
        }
        return gc;
    }

    /**
     * Update configuration of group in route table.
     *
     * @param groupId raft group id
     * @param confStr configuration string
     * @return true on success
     */
    public boolean updateConfiguration(final String groupId, final String confStr) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireTrue(!StringUtils.isBlank(confStr), "Blank configuration");

        final Configuration conf = new Configuration();
        if (conf.parse(confStr)) {
            return updateConfiguration(groupId, conf);
        } else {
            LOG.error("Fail to parse confStr: {}", confStr);
            return false;
        }
    }

    /**
     * Get the cached leader of the group, return it when found, null otherwise.
     * Make sure calls {@link #refreshLeader(CliClientService, String, int)} already
     * before invoke this method.
     *
     * @param groupId raft group id
     * @return peer of leader
     */
    public PeerId selectLeader(final String groupId) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");

        final GroupConf gc = this.groupConfTable.get(groupId);
        if (gc == null) {
            return null;
        }
        final StampedLock stampedLock = gc.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        PeerId leader = gc.leader;
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                leader = gc.leader;
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return leader;
    }

    /**
     * Update leader info.
     *
     * @param groupId raft group id
     * @param leader  peer of leader
     * @return true on success
     */
    public boolean updateLeader(final String groupId, final PeerId leader) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");

        if (leader != null) {
            // If leader presents, it should not be empty.
            Requires.requireTrue(!leader.isEmpty(), "Empty leader");
        }

        final GroupConf gc = getOrCreateGroupConf(groupId);
        final StampedLock stampedLock = gc.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            gc.leader = leader;
        } finally {
            stampedLock.unlockWrite(stamp);
        }
        return true;
    }

    /**
     * Update leader info.
     *
     * @param groupId   raft group id
     * @param leaderStr peer string of leader
     * @return true on success
     */
    public boolean updateLeader(final String groupId, final String leaderStr) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireTrue(!StringUtils.isBlank(leaderStr), "Blank leader");

        final PeerId leader = new PeerId();
        if (leader.parse(leaderStr)) {
            return updateLeader(groupId, leader);
        } else {
            LOG.error("Fail to parse leaderStr: {}", leaderStr);
            return false;
        }
    }

    /**
     * Get the configuration by groupId, returns null when not found.
     *
     * @param groupId raft group id
     * @return configuration of the group id
     */
    public Configuration getConfiguration(final String groupId) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");

        final GroupConf gc = this.groupConfTable.get(groupId);
        if (gc == null) {
            return null;
        }
        final StampedLock stampedLock = gc.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        Configuration conf = gc.conf;
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                conf = gc.conf;
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return conf;
    }

    /**
     * Blocking the thread until query_leader finishes.
     *
     * @param groupId   raft group id
     * @param timeoutMs timeout millis
     * @return operation status
     */
    public Status refreshLeader(final CliClientService cliClientService, final String groupId, final int timeoutMs)
                                                                                                                   throws InterruptedException,
                                                                                                                   TimeoutException {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireTrue(timeoutMs > 0, "Invalid timeout: " + timeoutMs);

        final Configuration conf = getConfiguration(groupId);
        if (conf == null) {
            return new Status(RaftError.ENOENT,
                "Group %s is not registered in RouteTable, forgot to call updateConfiguration?", groupId);
        }
        final Status st = Status.OK();

        TimeoutException timeoutException = null;
        for (final PeerId peer : conf) {
            final CliRequests.GetLeaderRequest.Builder rb = CliRequests.GetLeaderRequest.newBuilder();
            rb.setGroupId(groupId);
            rb.setPeerId(peer.toString());
            final CliRequests.GetLeaderRequest request = rb.build();
            if (!cliClientService.connect(peer.getEndpoint())) {
                if (st.isOk()) {
                    st.setError(-1, "Fail to init channel to %s", peer);
                } else {
                    final String savedMsg = st.getErrorMsg();
                    st.setError(-1, "%s, Fail to init channel to %s", savedMsg, peer);
                }
                continue;
            }
            final Future<Message> result = cliClientService.getLeader(peer.getEndpoint(), request, null);
            try {
                final Message msg = result.get(timeoutMs, TimeUnit.MILLISECONDS);
                if (msg instanceof RpcRequests.ErrorResponse) {
                    if (st.isOk()) {
                        st.setError(-1, ((RpcRequests.ErrorResponse) msg).getErrorMsg());
                    } else {
                        final String savedMsg = st.getErrorMsg();
                        st.setError(-1, "%s, %s", savedMsg, ((RpcRequests.ErrorResponse) msg).getErrorMsg());
                    }
                } else {
                    final CliRequests.GetLeaderResponse response = (CliRequests.GetLeaderResponse) msg;
                    updateLeader(groupId, response.getLeaderId());
                    return Status.OK();
                }
            } catch (final TimeoutException e) {
                timeoutException = e;
            } catch (final ExecutionException e) {
                if (st.isOk()) {
                    st.setError(-1, e.getMessage());
                } else {
                    final String savedMsg = st.getErrorMsg();
                    st.setError(-1, "%s, %s", savedMsg, e.getMessage());
                }
            }
        }
        if (timeoutException != null) {
            throw timeoutException;
        }

        return st;
    }

    public Status refreshConfiguration(final CliClientService cliClientService, final String groupId,
                                       final int timeoutMs) throws InterruptedException, TimeoutException {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireTrue(timeoutMs > 0, "Invalid timeout: " + timeoutMs);

        final Configuration conf = getConfiguration(groupId);
        if (conf == null) {
            return new Status(RaftError.ENOENT,
                "Group %s is not registered in RouteTable, forgot to call updateConfiguration?", groupId);
        }
        final Status st = Status.OK();
        PeerId leaderId = selectLeader(groupId);
        if (leaderId == null) {
            refreshLeader(cliClientService, groupId, timeoutMs);
            leaderId = selectLeader(groupId);
        }
        if (leaderId == null) {
            st.setError(-1, "Fail to get leader of group %s", groupId);
            return st;
        }
        if (!cliClientService.connect(leaderId.getEndpoint())) {
            st.setError(-1, "Fail to init channel to %s", leaderId);
            return st;
        }
        final CliRequests.GetPeersRequest.Builder rb = CliRequests.GetPeersRequest.newBuilder();
        rb.setGroupId(groupId);
        rb.setLeaderId(leaderId.toString());
        try {
            final Message result = cliClientService.getPeers(leaderId.getEndpoint(), rb.build(), null).get(timeoutMs,
                TimeUnit.MILLISECONDS);
            if (result instanceof CliRequests.GetPeersResponse) {
                final CliRequests.GetPeersResponse resp = (CliRequests.GetPeersResponse) result;
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }
                for (final String learnerIdStr : resp.getLearnersList()) {
                    final PeerId newLearner = new PeerId();
                    newLearner.parse(learnerIdStr);
                    newConf.addLearner(newLearner);
                }
                if (!conf.equals(newConf)) {
                    LOG.info("Configuration of replication group {} changed from {} to {}", groupId, conf, newConf);
                }
                updateConfiguration(groupId, newConf);
            } else {
                final RpcRequests.ErrorResponse resp = (RpcRequests.ErrorResponse) result;
                st.setError(resp.getErrorCode(), resp.getErrorMsg());
            }
        } catch (final Exception e) {
            st.setError(-1, e.getMessage());
        }
        return st;
    }

    /**
     * Reset the states.
     */
    public void reset() {
        this.groupConfTable.clear();
    }

    /**
     * Remove the group from route table.
     *
     * @param groupId raft group id
     * @return true on success
     */
    public boolean removeGroup(final String groupId) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");

        return this.groupConfTable.remove(groupId) != null;
    }

    @Override
    public String toString() {
        return "RouteTable{" + "groupConfTable=" + groupConfTable + '}';
    }

    private RouteTable() {
    }

    @Override
    public void describe(final Printer out) {
        out.println("RouteTable:") //
            .print("  ") //
            .println(toString());
    }

    private static class GroupConf {

        private final StampedLock stampedLock = new StampedLock();

        private Configuration     conf;
        private PeerId            leader;

        @Override
        public String toString() {
            return "GroupConf{" + "conf=" + conf + ", leader=" + leader + '}';
        }
    }
}
