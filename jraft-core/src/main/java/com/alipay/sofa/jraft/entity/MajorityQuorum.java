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
package com.alipay.sofa.jraft.entity;

import com.alipay.sofa.jraft.Quorum;
import com.alipay.sofa.jraft.conf.Configuration;

/**
 * A ballot to vote.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * <p>
 * 2018-Mar-15 2:29:11 PM
 */
public class MajorityQuorum extends Quorum {

    /**
     * Init the ballot with current conf and old conf.
     *
     * @param conf    current configuration
     * @param oldConf old configuration
     * @return true if init success
     */
    @Override
    public boolean init(final Configuration conf, final Configuration oldConf) {
        peers.clear();
        oldPeers.clear();
        quorum = oldQuorum = 0;

        int index = 0;
        if (conf != null) {
            for (final PeerId peer : conf) {
                peers.add(new UnfoundPeerId(peer, index++, false));
            }
        }

        quorum = peers.size() / 2 + 1;
        if (oldConf == null) {
            return true;
        }

        index = 0;
        for (final PeerId peer : oldConf) {
            oldPeers.add(new UnfoundPeerId(peer, index++, false));
        }

        oldQuorum = oldPeers.size() / 2 + 1;
        return true;
    }

    @Override
    public void grant(final PeerId peerId) {
        super.grant(peerId, new PosHint());
    }

}
