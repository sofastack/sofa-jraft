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

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author Akai
 */
public class NWRQuorum extends Quorum {

    protected Integer               readFactor;
    protected Integer               writeFactor;
    private static final String     defaultDecimalFactor = "0.1";
    private static final BigDecimal defaultDecimal       = new BigDecimal(defaultDecimalFactor);

    public NWRQuorum(Integer writeFactor, Integer readFactor) {
        this.writeFactor = writeFactor;
        this.readFactor = readFactor;
    }

    @Override
    public boolean init(Configuration conf, Configuration oldConf) {
        peers.clear();
        oldPeers.clear();
        quorum = oldQuorum = 0;
        int index = 0;

        if (conf != null) {
            for (final PeerId peer : conf) {
                peers.add(new UnfoundPeerId(peer, index++, false));
            }
        }

        BigDecimal writeFactorDecimal = defaultDecimal.multiply(new BigDecimal(writeFactor)).multiply(
            new BigDecimal(peers.size()));
        quorum = writeFactorDecimal.setScale(0, RoundingMode.CEILING).intValue();

        if (oldConf == null) {
            return true;
        }
        index = 0;
        for (final PeerId peer : oldConf) {
            oldPeers.add(new UnfoundPeerId(peer, index++, false));
        }

        BigDecimal writeFactorOldDecimal = defaultDecimal.multiply(new BigDecimal(writeFactor)).multiply(
            new BigDecimal(oldPeers.size()));
        oldQuorum = writeFactorOldDecimal.setScale(0, RoundingMode.CEILING).intValue();
        return true;
    }

    @Override
    public void grant(final PeerId peerId) {
        super.grant(peerId, new Quorum.PosHint());
    }

}
