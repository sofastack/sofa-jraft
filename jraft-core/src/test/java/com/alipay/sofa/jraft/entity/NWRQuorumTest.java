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

import com.alipay.sofa.jraft.JRaftUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Akai
 */
public class NWRQuorumTest {
    private NWRQuorum     nwrQuorum;
    private final Integer readFactor  = 4;
    private final Integer writeFactor = 6;

    @Before
    public void setup() {
        this.nwrQuorum = new NWRQuorum(writeFactor, readFactor);
        this.nwrQuorum.init(JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083,"
                                                        + "localhost:8084,localhost:8085"), null);

    }

    @Test
    public void testGrant() {
        PeerId peer1 = new PeerId("localhost", 8081);
        this.nwrQuorum.grant(peer1);
        assertFalse(this.nwrQuorum.isGranted());

        PeerId peer2 = new PeerId("localhost", 8082);
        this.nwrQuorum.grant(peer2);
        assertFalse(this.nwrQuorum.isGranted());

        PeerId unfoundPeer = new PeerId("localhost", 8086);
        this.nwrQuorum.grant(unfoundPeer);
        assertFalse(this.nwrQuorum.isGranted());

        PeerId peer3 = new PeerId("localhost", 8083);
        this.nwrQuorum.grant(peer3);
        assertTrue(this.nwrQuorum.isGranted());
    }
}
