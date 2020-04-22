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
package com.alipay.sofa.jraft.rpc.impl.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.Connection;
import com.alipay.remoting.ConnectionEventProcessor;
import com.alipay.remoting.ConnectionEventType;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.entity.PeerId;

/**
 * Client RPC service connection event processor for {@link ConnectionEventType#CONNECT}
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-12 10:21:22 AM
 */
public class ClientServiceConnectionEventProcessor implements ConnectionEventProcessor {

    private static final Logger   LOG = LoggerFactory.getLogger(ClientServiceConnectionEventProcessor.class);

    private final ReplicatorGroup rgGroup;

    public ClientServiceConnectionEventProcessor(ReplicatorGroup rgGroup) {
        super();
        this.rgGroup = rgGroup;
    }

    @Override
    public void onEvent(final String remoteAddr, final Connection conn) {
        final PeerId peer = new PeerId();
        if (peer.parse(remoteAddr)) {
            LOG.info("Peer {} is connected", peer);
            this.rgGroup.checkReplicator(peer, true);
        } else {
            LOG.error("Fail to parse peer: {}", remoteAddr);
        }
    }
}
