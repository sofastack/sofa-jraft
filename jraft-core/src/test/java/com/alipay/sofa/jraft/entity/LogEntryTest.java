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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LogEntryTest {

    @Test
    public void testEncodeDecodeWithoutData() {
        LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
        entry.setId(new LogId(100, 3));
        entry.setPeers(Arrays.asList(new PeerId("localhost", 99, 1), new PeerId("localhost", 100, 2)));
        assertNull(entry.getData());
        assertNull(entry.getOldPeers());

        byte[] content = entry.encode();

        assertNotNull(content);
        assertTrue(content.length > 0);
        assertEquals(LogEntry.MAGIC, content[0]);

        LogEntry nentry = new LogEntry();
        assertTrue(nentry.decode(content));

        assertEquals(100, nentry.getId().getIndex());
        assertEquals(3, nentry.getId().getTerm());
        Assert.assertEquals(EnumOutter.EntryType.ENTRY_TYPE_NO_OP, nentry.getType());
        assertEquals(2, nentry.getPeers().size());
        assertEquals("localhost:99:1", nentry.getPeers().get(0).toString());
        assertEquals("localhost:100:2", nentry.getPeers().get(1).toString());
        assertNull(nentry.getData());
        assertNull(nentry.getOldPeers());
    }

    @Test
    public void testEncodeDecodeWithData() {
        ByteBuffer buf = ByteBuffer.wrap("hello".getBytes());
        LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
        entry.setId(new LogId(100, 3));
        entry.setData(buf);
        entry.setPeers(Arrays.asList(new PeerId("localhost", 99, 1), new PeerId("localhost", 100, 2)));
        assertEquals(buf, entry.getData());

        byte[] content = entry.encode();

        assertNotNull(content);
        assertTrue(content.length > 0);
        assertEquals(LogEntry.MAGIC, content[0]);

        LogEntry nentry = new LogEntry();
        assertTrue(nentry.decode(content));

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
