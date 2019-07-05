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
package com.alipay.sofa.jraft.storage.impl;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-11 4:50:23 PM
 */
@RunWith(MockitoJUnitRunner.class)
public class LocalRaftMetaStorageTest extends BaseStorageTest {
    private RaftMetaStorage raftMetaStorage;

    @Mock
    private NodeImpl        node;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.raftMetaStorage = new LocalRaftMetaStorage(this.path, new RaftOptions());
        Mockito.when(this.node.getNodeMetrics()).thenReturn(null);
        assertTrue(this.raftMetaStorage.init(newOptions()));
    }

    private RaftMetaStorageOptions newOptions() {
        RaftMetaStorageOptions raftMetaStorageOptions = new RaftMetaStorageOptions();
        raftMetaStorageOptions.setNode(this.node);
        return raftMetaStorageOptions;
    }

    @Test
    public void testGetAndSetReload() {
        assertEquals(0, this.raftMetaStorage.getTerm());
        assertTrue(this.raftMetaStorage.getVotedFor().isEmpty());

        this.raftMetaStorage.setTerm(99);
        assertEquals(99, this.raftMetaStorage.getTerm());
        assertTrue(this.raftMetaStorage.getVotedFor().isEmpty());

        assertTrue(this.raftMetaStorage.setVotedFor(new PeerId("localhost", 8081)));
        assertEquals(99, this.raftMetaStorage.getTerm());
        Assert.assertEquals(new PeerId("localhost", 8081), this.raftMetaStorage.getVotedFor());

        assertTrue(this.raftMetaStorage.setTermAndVotedFor(100, new PeerId("localhost", 8083)));
        assertEquals(100, this.raftMetaStorage.getTerm());
        Assert.assertEquals(new PeerId("localhost", 8083), this.raftMetaStorage.getVotedFor());

        this.raftMetaStorage = new LocalRaftMetaStorage(this.path, new RaftOptions());
        Mockito.when(this.node.getNodeMetrics()).thenReturn(null);
        this.raftMetaStorage.init(newOptions());
        assertEquals(100, this.raftMetaStorage.getTerm());
        Assert.assertEquals(new PeerId("localhost", 8083), this.raftMetaStorage.getVotedFor());
    }

    @Test
    public void testSaveFail() throws IOException {
        FileUtils.deleteDirectory(new File(this.path));
        assertFalse(this.raftMetaStorage.setVotedFor(new PeerId("localhost", 8081)));
        Mockito.verify(this.node, Mockito.times(1)).onError((RaftException) Mockito.any());
    }
}
