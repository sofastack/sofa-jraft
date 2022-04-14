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
package com.alipay.sofa.jraft.storage.db;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.util.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author hzh (642256541@qq.com)
 */
public class ConfDBTest extends BaseStorageTest {
    private ConfDB               confDB;
    private String               confStorePath;
    private LogEntryCodecFactory logEntryCodecFactory;
    private LogEntryDecoder      decoder;
    private LogEntryEncoder      encoder;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        this.confStorePath = this.path + File.separator + "conf";
        FileUtils.forceMkdir(new File(this.confStorePath));
        this.logEntryCodecFactory = LogEntryV2CodecFactory.getInstance();
        decoder = this.logEntryCodecFactory.decoder();
        encoder = this.logEntryCodecFactory.encoder();
        this.init();
    }

    public void init() {
        this.confDB = new ConfDB(this.confStorePath);
        this.confDB.init(this.logStoreFactory);
    }

    @After
    public void teardown() throws Exception {
        this.confDB.shutdown();
        super.teardown();
    }

    @Test
    public void testAppendConfAndIter() throws Exception {
        this.confDB.startServiceManager();
        final LogEntry confEntry1 = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        confEntry1.setId(new LogId(1, 1));
        final List<PeerId> conf1Peers = JRaftUtils.getConfiguration("localhost:8081,localhost:8082").listPeers();
        confEntry1.setPeers(conf1Peers);

        final LogEntry confEntry2 = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        confEntry2.setId(new LogId(2, 2));
        final List<PeerId> conf2Peers = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083")
            .listPeers();
        confEntry2.setPeers(conf2Peers);
        {

            this.confDB.appendLogAsync(1, this.encoder.encode(confEntry1));
            final Pair<Integer, Long> posPair = this.confDB.appendLogAsync(2, this.encoder.encode(confEntry2));
            this.confDB.waitForFlush(posPair.getSecond(), 100);
        }
        {
            final AbstractDB.LogEntryIterator iterator = this.confDB.iterator(this.decoder);
            final LogEntry conf1 = iterator.next();
            assertEquals(toString(conf1Peers), toString(conf1.getPeers()));

            final LogEntry conf2 = iterator.next();
            assertEquals(toString(conf2Peers), toString(conf2.getPeers()));

            assertNull(iterator.next());
        }
    }

    public String toString(final List<PeerId> peers) {
        final StringBuilder sb = new StringBuilder();
        int i = 0;
        int size = peers.size();
        for (final PeerId peer : peers) {
            sb.append(peer);
            if (i < size - 1) {
                sb.append(",");
            }
            i++;
        }
        return sb.toString();
    }
}
