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
package com.alipay.sofa.jraft.rhea;

import java.util.List;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;

/**
 *
 * @author jiachun.fjc
 */
public final class JRaftHelper {

    public static String getJRaftGroupId(final String clusterName, final long regionId) {
        Requires.requireNonNull(clusterName, "clusterName");
        return clusterName + "-" + regionId;
    }

    public static PeerId toJRaftPeerId(final Peer peer) {
        Requires.requireNonNull(peer, "peer");
        final Endpoint endpoint = peer.getEndpoint();
        Requires.requireNonNull(endpoint, "peer.endpoint");
        return new PeerId(endpoint, 0);
    }

    public static List<PeerId> toJRaftPeerIdList(final List<Peer> peerList) {
        if (peerList == null) {
            return null;
        }
        final List<PeerId> peerIdList = Lists.newArrayListWithCapacity(peerList.size());
        for (final Peer peer : peerList) {
            peerIdList.add(toJRaftPeerId(peer));
        }
        return peerIdList;
    }

    public static Peer toPeer(final PeerId peerId) {
        Requires.requireNonNull(peerId, "peerId");
        final Endpoint endpoint = peerId.getEndpoint();
        Requires.requireNonNull(endpoint, "peerId.endpoint");
        final Peer peer = new Peer();
        peer.setId(-1);
        peer.setStoreId(-1);
        peer.setEndpoint(endpoint.copy());
        return peer;
    }

    public static List<Peer> toPeerList(final List<PeerId> peerIdList) {
        if (peerIdList == null) {
            return null;
        }
        final List<Peer> peerList = Lists.newArrayListWithCapacity(peerIdList.size());
        for (final PeerId peerId : peerIdList) {
            peerList.add(toPeer(peerId));
        }
        return peerList;
    }
}
