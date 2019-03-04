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
package com.alipay.sofa.jraft.conf;

import org.junit.Test;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.entity.PeerId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class ConfigurationTest {

    @Test
    public void testToStringParseStuff() {
        final String confStr = "localhost:8081,localhost:8082,localhost:8083";
        final Configuration conf = JRaftUtils.getConfiguration(confStr);
        assertEquals(3, conf.size());
        for (final PeerId peer : conf) {
            assertTrue(peer.toString().startsWith("localhost:80"));
        }
        assertFalse(conf.isEmpty());
        assertEquals(confStr, conf.toString());
        final Configuration newConf = new Configuration();
        assertTrue(newConf.parse(conf.toString()));
        assertEquals(3, newConf.getPeerSet().size());
        assertTrue(newConf.contains(new PeerId("localhost", 8081)));
        assertTrue(newConf.contains(new PeerId("localhost", 8082)));
        assertTrue(newConf.contains(new PeerId("localhost", 8083)));
        assertEquals(confStr, newConf.toString());
        assertEquals(conf.hashCode(), newConf.hashCode());
        assertEquals(conf, newConf);
    }

    @Test
    public void testCopy() {
        final Configuration conf = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083");
        final Configuration copied = conf.copy();
        assertEquals(conf, copied);
        assertNotSame(conf, copied);
        assertEquals(copied.size(), 3);
        assertEquals("localhost:8081,localhost:8082,localhost:8083", copied.toString());

        final PeerId newPeer = new PeerId("localhost", 8084);
        conf.addPeer(newPeer);
        assertEquals(copied.size(), 3);
        assertEquals(conf.size(), 4);
        assertTrue(conf.contains(newPeer));
        assertFalse(copied.contains(newPeer));
    }

    @Test
    public void testReset() {
        final Configuration conf = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083");
        assertFalse(conf.isEmpty());
        conf.reset();
        assertTrue(conf.isEmpty());
        assertTrue(conf.getPeerSet().isEmpty());
    }

    @Test
    public void testDiff() {
        final Configuration conf1 = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083");
        final Configuration conf2 = JRaftUtils
            .getConfiguration("localhost:8081,localhost:8083,localhost:8085,localhost:8086");
        final Configuration included = new Configuration();
        final Configuration excluded = new Configuration();
        conf1.diff(conf2, included, excluded);
        assertEquals("localhost:8082", included.toString());
        assertEquals("localhost:8085,localhost:8086", excluded.toString());
    }
}
