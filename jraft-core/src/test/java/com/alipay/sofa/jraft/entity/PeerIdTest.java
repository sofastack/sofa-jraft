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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PeerIdTest {

    @Test
    public void testToStringParse() {
        final PeerId peer = new PeerId("192.168.1.1", 8081, 0);
        assertEquals("192.168.1.1:8081", peer.toString());

        final PeerId pp = new PeerId();
        assertTrue(pp.parse(peer.toString()));
        assertEquals(8081, pp.getPort());
        assertEquals("192.168.1.1", pp.getIp());
        assertEquals(0, pp.getIdx());
        assertEquals(pp, peer);
        assertEquals(pp.hashCode(), peer.hashCode());
    }

    @Test
    public void testIdx() {
        final PeerId peer = new PeerId("192.168.1.1", 8081, 1);
        assertEquals("192.168.1.1:8081:1", peer.toString());
        assertFalse(peer.isEmpty());

        final PeerId pp = new PeerId();
        assertTrue(pp.parse(peer.toString()));
        assertEquals(8081, pp.getPort());
        assertEquals("192.168.1.1", pp.getIp());
        assertEquals(1, pp.getIdx());
        assertEquals(pp, peer);
        assertEquals(pp.hashCode(), peer.hashCode());
    }

    @Test
    public void testParseFail() {
        final PeerId peer = new PeerId();
        assertTrue(peer.isEmpty());
        assertFalse(peer.parse("localhsot:2:3:4"));
        assertTrue(peer.isEmpty());
    }

    @Test
    public void testEmptyPeer() {
        PeerId peer = new PeerId("192.168.1.1", 8081, 1);
        assertFalse(peer.isEmpty());
        peer = PeerId.emptyPeer();
        assertTrue(peer.isEmpty());
    }

    @Test
    public void testChecksum() {
        PeerId peer = new PeerId("192.168.1.1", 8081, 1);
        long c = peer.checksum();
        assertTrue(c != 0);
        assertEquals(c, peer.checksum());
    }
}
