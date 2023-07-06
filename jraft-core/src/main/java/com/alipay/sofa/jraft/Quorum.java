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

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Akai
 */
public abstract class Quorum {
    private static final Logger                LOG      = LoggerFactory.getLogger(Quorum.class);

    protected final List<Quorum.UnfoundPeerId> peers    = new ArrayList<>();

    protected int                              quorum;

    protected final List<Quorum.UnfoundPeerId> oldPeers = new ArrayList<>();
    protected int                              oldQuorum;

    public static final class PosHint {
        int pos0 = -1; // position in current peers
        int pos1 = -1; // position in old peers
    }

    public static class UnfoundPeerId {
        PeerId  peerId;
        boolean found;
        int     index;

        public UnfoundPeerId(PeerId peerId, int index, boolean found) {
            super();
            this.peerId = peerId;
            this.index = index;
            this.found = found;
        }
    }

    public abstract boolean init(final Configuration conf, final Configuration oldConf);

    public abstract void grant(final PeerId peerId);

    private UnfoundPeerId findPeer(final PeerId peerId, final List<UnfoundPeerId> peers, final int posHint) {
        if (posHint < 0 || posHint >= peers.size() || !peers.get(posHint).peerId.equals(peerId)) {
            for (final UnfoundPeerId ufp : peers) {
                if (ufp.peerId.equals(peerId)) {
                    return ufp;
                }
            }
            return null;
        }

        return peers.get(posHint);
    }

    /**
     * Returns true when the ballot is granted.
     *
     * @return true if the ballot is granted
     */
    public boolean isGranted() {
        return quorum <= 0 && oldQuorum <= 0;
    }

    public PosHint grant(final PeerId peerId, final PosHint hint) {
        UnfoundPeerId peer = findPeer(peerId, this.peers, hint.pos0);
        if (peer != null) {
            if (!peer.found) {
                peer.found = true;
                this.quorum--;
            }
            hint.pos0 = peer.index;
        } else {
            hint.pos0 = -1;
        }
        if (this.oldPeers.isEmpty()) {
            hint.pos1 = -1;
            return hint;
        }
        peer = findPeer(peerId, this.oldPeers, hint.pos1);
        if (peer != null) {
            if (!peer.found) {
                peer.found = true;
                this.oldQuorum--;
            }
            hint.pos1 = peer.index;
        } else {
            hint.pos1 = -1;
        }

        return hint;
    }
}
