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
package com.alipay.sofa.jraft.core;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.Quorum;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.BallotFactory;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.test.TestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Akai
 */
public class FlexibleRaftTest {
    private String           dataPath;
    private TestCluster      cluster;
    private final String     groupId           = "FlexibleRaftTest";

    private Integer          readFactor;
    private Integer          writeFactor;
    private Boolean          enableFlexible;
    private CliService       cliService;

    private Configuration    conf;

    @Rule
    public TestName          testName          = new TestName();
    private static final int LEARNER_PORT_STEP = 100;

    @Before
    public void setup() throws Exception {
        System.out.println(">>>>>>>>>>>>>>> Start test method: " + this.testName.getMethodName());
        this.dataPath = TestUtils.mkTempDir();
        this.readFactor = 4;
        this.writeFactor = 6;
        this.enableFlexible = true;
        FileUtils.forceMkdir(new File(this.dataPath));
        assertEquals(NodeImpl.GLOBAL_NUM_NODES.get(), 0);
        final List<PeerId> peers = TestUtils.generatePeers(5);

        final LinkedHashSet<PeerId> learners = new LinkedHashSet<>();
        //2 learners
        for (int i = 0; i < 2; i++) {
            learners.add(new PeerId(TestUtils.getMyIp(), TestUtils.INIT_PORT + LEARNER_PORT_STEP + i));
        }

        this.cluster = new TestCluster(this.groupId, this.dataPath, peers, learners, 300);
        for (final PeerId peer : peers) {
            this.cluster.startWithFlexible(peer.getEndpoint(), this.readFactor, this.writeFactor);
        }

        for (final PeerId peer : learners) {
            this.cluster.startLearnerWithFlexible(peer, this.readFactor, this.writeFactor);
        }

        this.cluster.waitLeader();
        Thread.sleep(1000);
        this.cliService = new CliServiceImpl();
        Quorum quorum = BallotFactory.buildFlexibleQuorum(this.readFactor, this.writeFactor, peers.size());
        this.conf = new Configuration(peers, learners, quorum, this.readFactor, this.writeFactor, this.enableFlexible);

        assertTrue(this.cliService.init(new CliOptions()));
    }

    @Test
    public void testResetFactor() {

        int changeReadFactor = 8;
        int changeWriteFactor = 2;

        assertTrue(this.cliService.resetFactor(this.groupId, this.conf, changeReadFactor, changeWriteFactor).isOk());
    }

    @After
    public void teardown() throws Exception {
        this.cliService.shutdown();
        this.cluster.stopAll();
        if (NodeImpl.GLOBAL_NUM_NODES.get() > 0) {
            Thread.sleep(1000);
            assertEquals(NodeImpl.GLOBAL_NUM_NODES.get(), 0);
        }
        FileUtils.deleteDirectory(new File(this.dataPath));
        NodeManager.getInstance().clear();
        RouteTable.getInstance().reset();
        System.out.println(">>>>>>>>>>>>>>> End test method: " + this.testName.getMethodName());
    }
}
