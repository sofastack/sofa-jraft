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
package com.alipay.sofa.jraft.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.JRaftException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.CliClientService;
import com.alipay.sofa.jraft.rpc.CliRequests.AddPeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.AddPeerResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.ChangePeersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.ChangePeersResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.GetLeaderRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.GetLeaderResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.GetPeersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.GetPeersResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.RemovePeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.RemovePeerResponse;
import com.alipay.sofa.jraft.rpc.CliRequests.ResetPeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.SnapshotRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.TransferLeaderRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ErrorResponse;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import com.alipay.sofa.jraft.util.Requires;
import com.google.protobuf.Message;

/**
 * Cli service implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 4:12:06 PM
 */
public class CliServiceImpl implements CliService {

    private static final Logger LOG = LoggerFactory.getLogger(CliServiceImpl.class);

    private CliOptions          cliOptions;
    private CliClientService    cliClientService;

    @Override
    public synchronized boolean init(CliOptions opts) {
        Requires.requireNonNull(opts, "Null cli options");

        if (this.cliClientService != null) {
            return true;
        }
        this.cliOptions = opts;
        this.cliClientService = new BoltCliClientService();
        return this.cliClientService.init(this.cliOptions);
    }

    @Override
    public synchronized void shutdown() {
        if (this.cliClientService == null) {
            return;
        }
        this.cliClientService.shutdown();
        this.cliClientService = null;
    }

    @Override
    public Status addPeer(final String groupId, final Configuration conf, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(peer, "Null peer");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }
        final AddPeerRequest.Builder rb = AddPeerRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString()) //
            .setPeerId(peer.toString());

        try {
            final Message result = this.cliClientService.addPeer(leaderId.getEndpoint(), rb.build(), null).get();
            if (result instanceof AddPeerResponse) {
                final AddPeerResponse resp = (AddPeerResponse) result;
                final Configuration oldConf = new Configuration();
                for (final String peerIdStr : resp.getOldPeersList()) {
                    final PeerId oldPeer = new PeerId();
                    oldPeer.parse(peerIdStr);
                    oldConf.addPeer(oldPeer);
                }
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getNewPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }

                LOG.info("Configuration of replication group {} changed from {} to {}.", groupId, oldConf, newConf);
                return Status.OK();
            } else {
                return statusFromResponse(result);
            }

        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    private Status statusFromResponse(final Message result) {
        final ErrorResponse resp = (ErrorResponse) result;
        return new Status(resp.getErrorCode(), resp.getErrorMsg());
    }

    @Override
    public Status removePeer(final String groupId, final Configuration conf, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(peer, "Null peer");
        Requires.requireTrue(!peer.isEmpty(), "Removing peer is blank");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }

        final RemovePeerRequest.Builder rb = RemovePeerRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString()) //
            .setPeerId(peer.toString());

        try {
            final Message result = this.cliClientService.removePeer(leaderId.getEndpoint(), rb.build(), null).get();
            if (result instanceof RemovePeerResponse) {
                final RemovePeerResponse resp = (RemovePeerResponse) result;
                final Configuration oldConf = new Configuration();
                for (final String peerIdStr : resp.getOldPeersList()) {
                    final PeerId oldPeer = new PeerId();
                    oldPeer.parse(peerIdStr);
                    oldConf.addPeer(oldPeer);
                }
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getNewPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }

                LOG.info("Configuration of replication group {} changed from {} to {}", groupId, oldConf, newConf);
                return Status.OK();
            } else {
                return statusFromResponse(result);

            }
        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    // TODO refactor addPeer/removePeer/changePeers/transferLeader, remove duplicated code.
    @Override
    public Status changePeers(final String groupId, final Configuration conf, final Configuration newPeers) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(newPeers, "Null new peers");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }

        final ChangePeersRequest.Builder rb = ChangePeersRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString());
        for (final PeerId peer : newPeers) {
            rb.addNewPeers(peer.toString());
        }

        try {
            final Message result = this.cliClientService.changePeers(leaderId.getEndpoint(), rb.build(), null).get();
            if (result instanceof ChangePeersResponse) {
                final ChangePeersResponse resp = (ChangePeersResponse) result;
                final Configuration oldConf = new Configuration();
                for (final String peerIdStr : resp.getOldPeersList()) {
                    final PeerId oldPeer = new PeerId();
                    oldPeer.parse(peerIdStr);
                    oldConf.addPeer(oldPeer);
                }
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getNewPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }

                LOG.info("Configuration of replication group {} changed from {} to {}", groupId, oldConf, newConf);
                return Status.OK();
            } else {
                return statusFromResponse(result);

            }
        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    @Override
    public Status resetPeer(final String groupId, final PeerId peerId, final Configuration newPeers) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(peerId, "Null peerId");
        Requires.requireNonNull(newPeers, "Null new peers");

        if (!this.cliClientService.connect(peerId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to %s", peerId);
        }

        final ResetPeerRequest.Builder rb = ResetPeerRequest.newBuilder() //
            .setGroupId(groupId) //
            .setPeerId(peerId.toString());
        for (final PeerId peer : newPeers) {
            rb.addNewPeers(peer.toString());
        }

        try {
            final Message result = this.cliClientService.resetPeer(peerId.getEndpoint(), rb.build(), null).get();
            return statusFromResponse(result);
        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    @Override
    public Status transferLeader(final String groupId, final Configuration conf, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(peer, "Null peer");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }

        final TransferLeaderRequest.Builder rb = TransferLeaderRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString());
        if (!peer.isEmpty()) {
            rb.setPeerId(peer.toString());
        }

        try {
            final Message result = this.cliClientService.transferLeader(leaderId.getEndpoint(), rb.build(), null).get();
            return statusFromResponse(result);
        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    @Override
    public Status snapshot(final String groupId, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(peer, "Null peer");

        if (!this.cliClientService.connect(peer.getEndpoint())) {
            return new Status(-1, "Fail to init channel to %s", peer);
        }

        final SnapshotRequest.Builder rb = SnapshotRequest.newBuilder() //
            .setGroupId(groupId) //
            .setPeerId(peer.toString());

        try {
            final Message result = this.cliClientService.snapshot(peer.getEndpoint(), rb.build(), null).get();
            return statusFromResponse(result);
        } catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    @Override
    public Status getLeader(final String groupId, final Configuration conf, final PeerId leaderId) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(leaderId, "Null leader id");

        if (conf == null || conf.isEmpty()) {
            return new Status(RaftError.EINVAL, "Empty group configuration");
        }

        final Status st = new Status(-1, "Fail to get leader of group %s", groupId);
        for (final PeerId peer : conf) {
            if (!this.cliClientService.connect(peer.getEndpoint())) {
                LOG.error("Fail to connect peer {} to get leader for group {}.", groupId);
                continue;
            }

            final GetLeaderRequest.Builder rb = GetLeaderRequest.newBuilder() //
                .setGroupId(groupId) //
                .setPeerId(peer.toString());

            final Future<Message> result = this.cliClientService.getLeader(peer.getEndpoint(), rb.build(), null);
            try {

                final Message msg = result.get(
                    this.cliOptions.getTimeoutMs() <= 0 ? this.cliOptions.getRpcDefaultTimeout() : this.cliOptions
                        .getTimeoutMs(), TimeUnit.MILLISECONDS);
                if (msg instanceof ErrorResponse) {
                    if (st.isOk()) {
                        st.setError(-1, ((ErrorResponse) msg).getErrorMsg());
                    } else {
                        final String savedMsg = st.getErrorMsg();
                        st.setError(-1, "%s, %s", savedMsg, ((ErrorResponse) msg).getErrorMsg());
                    }
                } else {
                    final GetLeaderResponse response = (GetLeaderResponse) msg;
                    if (leaderId.parse(response.getLeaderId())) {
                        break;
                    }
                }
            } catch (final Exception e) {
                if (st.isOk()) {
                    st.setError(-1, e.getMessage());
                } else {
                    final String savedMsg = st.getErrorMsg();
                    st.setError(-1, "%s, %s", savedMsg, e.getMessage());
                }
            }
        }

        if (leaderId.isEmpty()) {
            return st;
        }
        return Status.OK();
    }

    @Override
    public List<PeerId> getPeers(final String groupId, final Configuration conf) {
        return getPeers(groupId, conf, false);
    }

    @Override
    public List<PeerId> getAlivePeers(final String groupId, final Configuration conf) {
        return getPeers(groupId, conf, true);
    }

    private List<PeerId> getPeers(final String groupId, final Configuration conf, final boolean onlyGetAlive) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null conf");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            throw new IllegalStateException(st.getErrorMsg());
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            throw new IllegalStateException("Fail to init channel to leader " + leaderId);
        }

        final GetPeersRequest.Builder rb = GetPeersRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString()) //
            .setOnlyAlive(onlyGetAlive);

        try {
            final Message result = this.cliClientService.getPeers(leaderId.getEndpoint(), rb.build(), null).get(
                this.cliOptions.getTimeoutMs() <= 0 ? this.cliOptions.getRpcDefaultTimeout()
                    : this.cliOptions.getTimeoutMs(), TimeUnit.MILLISECONDS);
            if (result instanceof GetPeersResponse) {
                final GetPeersResponse resp = (GetPeersResponse) result;
                final List<PeerId> peerIdList = new ArrayList<>();
                for (final String peerIdStr : resp.getPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    peerIdList.add(newPeer);
                }
                return peerIdList;
            } else {
                final ErrorResponse resp = (ErrorResponse) result;
                throw new JRaftException(resp.getErrorMsg());
            }
        } catch (final JRaftException e) {
            throw e;
        } catch (final Exception e) {
            throw new JRaftException(e);
        }
    }

    public CliClientService getCliClientService() {
        return cliClientService;
    }
}
