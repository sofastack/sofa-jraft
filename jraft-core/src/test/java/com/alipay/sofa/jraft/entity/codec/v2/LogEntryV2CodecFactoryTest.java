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
package com.alipay.sofa.jraft.entity.codec.v2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.codec.BaseLogEntryCodecFactoryTest;
import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.entity.codec.v1.V1Encoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LogEntryV2CodecFactoryTest extends BaseLogEntryCodecFactoryTest {

    @Override
    protected LogEntryCodecFactory newFactory() {
        LogEntryCodecFactory factory = LogEntryV2CodecFactory.getInstance();
        return factory;
    }

    @Test
    public void testEncodeDecodeWithLearners() {
        LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
        entry.setId(new LogId(100, 3));
        entry.setPeers(Arrays.asList(new PeerId("localhost", 99, 1), new PeerId("localhost", 100, 2)));
        List<PeerId> theLearners = createLearners("192.168.1.1:8081", "192.168.1.2:8081");
        entry.setLearners(theLearners);
        assertSame(entry.getData(), LogEntry.EMPTY_DATA);
        assertNull(entry.getOldPeers());

        byte[] content = this.encoder.encode(entry);

        assertNotNull(content);
        assertTrue(content.length > 0);

        LogEntry nentry = this.decoder.decode(content);
        assertNotNull(nentry);
        assertNull(nentry.getOldLearners());
        assertPeersAndLearners(theLearners, nentry);

        // test old learners
        List<PeerId> theOldLearners = createLearners("192.168.1.1:8081");
        entry.setOldLearners(theOldLearners);
        content = this.encoder.encode(entry);
        assertNotNull(content);
        assertTrue(content.length > 0);
        nentry = this.decoder.decode(content);
        assertNotNull(nentry);
        assertPeersAndLearners(theLearners, nentry);
        List<PeerId> oldLearners = nentry.getOldLearners();
        assertNotNull(oldLearners);
        assertEquals(1, oldLearners.size());
        assertEquals(oldLearners, theOldLearners);

    }

    private void assertPeersAndLearners(final List<PeerId> theLearners, final LogEntry nentry) {
        assertEquals(100, nentry.getId().getIndex());
        assertEquals(3, nentry.getId().getTerm());
        Assert.assertEquals(EnumOutter.EntryType.ENTRY_TYPE_NO_OP, nentry.getType());
        assertEquals(2, nentry.getPeers().size());
        assertEquals("localhost:99:1", nentry.getPeers().get(0).toString());
        assertEquals("localhost:100:2", nentry.getPeers().get(1).toString());
        assertSame(nentry.getData(), LogEntry.EMPTY_DATA);
        assertNull(nentry.getOldPeers());

        assertTrue(nentry.hasLearners());
        List<PeerId> learners = nentry.getLearners();
        assertNotNull(learners);
        assertEquals(2, learners.size());
        assertEquals(learners, theLearners);
    }

    private List<PeerId> createLearners(final String... peers) {
        List<PeerId> ret = new ArrayList<>();
        for (String s : peers) {
            PeerId e = new PeerId();
            e.parse(s);
            ret.add(e);
        }
        return ret;
    }

    @Test
    public void testDecodeV1LogEntry() {

        ByteBuffer buf = ByteBuffer.wrap("hello".getBytes());
        LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
        entry.setId(new LogId(100, 3));
        entry.setData(buf);
        entry.setPeers(Arrays.asList(new PeerId("localhost", 99, 1), new PeerId("localhost", 100, 2)));
        assertEquals(buf, entry.getData());

        byte[] content = V1Encoder.INSTANCE.encode(entry);
        assertNotNull(content);
        assertTrue(content.length > 0);

        // Decode by auto detect decoder
        LogEntry nentry = this.decoder.decode(content);
        assertNotNull(nentry);

        assertEquals(100, nentry.getId().getIndex());
        assertEquals(3, nentry.getId().getTerm());

        assertEquals(2, nentry.getPeers().size());
        assertEquals("localhost:99:1", nentry.getPeers().get(0).toString());
        assertEquals("localhost:100:2", nentry.getPeers().get(1).toString());
        assertEquals(buf, nentry.getData());
        assertEquals(0, nentry.getData().position());
        assertEquals(5, nentry.getData().remaining());
        assertNull(nentry.getOldPeers());
    }
}
