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
public class FlexibleQuorumTest {
    private FlexibleQuorum flexibleQuorum;
    private final Integer  readFactor  = 4;
    private final Integer  writeFactor = 6;

    @Before
    public void setup() {
        this.flexibleQuorum = new FlexibleQuorum(writeFactor, readFactor);
        this.flexibleQuorum.init(JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083,"
                                                             + "localhost:8084,localhost:8085"), null);

    }

    @Test
    public void testGrant() {
        PeerId peer1 = new PeerId("localhost", 8081);
        this.flexibleQuorum.grant(peer1);
        assertFalse(this.flexibleQuorum.isGranted());

        PeerId peer2 = new PeerId("localhost", 8082);
        this.flexibleQuorum.grant(peer2);
        assertFalse(this.flexibleQuorum.isGranted());

        PeerId unfoundPeer = new PeerId("localhost", 8086);
        this.flexibleQuorum.grant(unfoundPeer);
        assertFalse(this.flexibleQuorum.isGranted());

        PeerId peer3 = new PeerId("localhost", 8083);
        this.flexibleQuorum.grant(peer3);
        assertTrue(this.flexibleQuorum.isGranted());
    }
}
