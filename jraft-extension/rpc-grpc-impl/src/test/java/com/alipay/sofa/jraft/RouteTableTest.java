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
package com.alipay.sofa.jraft;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.TestCluster;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.CliClientService;
import com.alipay.sofa.jraft.rpc.CliRequests;
import com.alipay.sofa.jraft.rpc.impl.GrpcResponseFactory;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(value = MockitoJUnitRunner.class)
public class RouteTableTest {

    private String       dataPath;

    private TestCluster  cluster;
    private final String groupId = "RouteTableTest";

    CliClientServiceImpl cliClientService;
    @Mock
    CliClientService     mockCliClientService;

    @Before
    public void setup() throws Exception {
        cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());
        this.dataPath = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.dataPath));
        assertEquals(NodeImpl.GLOBAL_NUM_NODES.get(), 0);
        final List<PeerId> peers = TestUtils.generatePeers(3);

        cluster = new TestCluster(groupId, dataPath, peers);
        for (final PeerId peer : peers) {
            cluster.start(peer.getEndpoint());
        }
        cluster.waitLeader();
    }

    @After
    public void teardown() throws Exception {
        cliClientService.shutdown();
        cluster.stopAll();
        if (NodeImpl.GLOBAL_NUM_NODES.get() > 0) {
            Thread.sleep(1000);
            assertEquals(NodeImpl.GLOBAL_NUM_NODES.get(), 0);
        }
        FileUtils.deleteDirectory(new File(this.dataPath));
        NodeManager.getInstance().clear();
        RouteTable.getInstance().reset();
    }

    @Test
    public void testUpdateConfSelectLeader() throws Exception {
        final RouteTable rt = RouteTable.getInstance();
        assertNull(rt.getConfiguration(groupId));
        rt.updateConfiguration(groupId, new Configuration(cluster.getPeers()));
        assertEquals(rt.getConfiguration(groupId), new Configuration(cluster.getPeers()));
        assertNull(rt.selectLeader(groupId));
        assertTrue(rt.refreshLeader(cliClientService, groupId, 10000).isOk());

        final PeerId leader = rt.selectLeader(groupId);
        assertEquals(leader, cluster.getLeader().getNodeId().getPeerId());
    }

    @Test
    public void testUpdateLeaderNull() throws Exception {
        this.testUpdateConfSelectLeader();
        final RouteTable rt = RouteTable.getInstance();
        rt.updateLeader(groupId, (PeerId) null);
        assertNull(rt.selectLeader(groupId));
        assertTrue(rt.refreshLeader(cliClientService, groupId, 10000).isOk());

        final PeerId leader = rt.selectLeader(groupId);
        assertEquals(leader, cluster.getLeader().getNodeId().getPeerId());
    }

    @Test
    public void testRefreshLeaderWhenLeaderStops() throws Exception {
        final RouteTable rt = RouteTable.getInstance();
        testUpdateConfSelectLeader();
        PeerId leader = rt.selectLeader(groupId);
        this.cluster.stop(leader.getEndpoint());
        this.cluster.waitLeader();
        final PeerId oldLeader = leader.copy();

        assertTrue(rt.refreshLeader(cliClientService, groupId, 10000).isOk());
        leader = rt.selectLeader(groupId);
        assertNotEquals(leader, oldLeader);
        assertEquals(leader, cluster.getLeader().getNodeId().getPeerId());
    }

    @Test
    public void testRefreshLeaderWhenFirstPeerDown() throws Exception {
        final RouteTable rt = RouteTable.getInstance();
        rt.updateConfiguration(groupId, new Configuration(cluster.getPeers()));
        assertTrue(rt.refreshLeader(cliClientService, groupId, 10000).isOk());
        cluster.stop(cluster.getPeers().get(0).getEndpoint());
        Thread.sleep(1000);
        this.cluster.waitLeader();
        assertTrue(rt.refreshLeader(cliClientService, groupId, 10000).isOk());
    }

    @Test
    public void testRefreshFail() throws Exception {
        cluster.stopAll();
        final RouteTable rt = RouteTable.getInstance();
        rt.updateConfiguration(groupId, new Configuration(cluster.getPeers()));
        final Status status = rt.refreshLeader(cliClientService, groupId, 5000);
        assertFalse(status.isOk());
        assertTrue(status.getErrorMsg().contains("Fail to init channel"));
    }

    @Test
    public void testRefreshConfiguration() throws Exception {
        final RouteTable rt = RouteTable.getInstance();
        final List<PeerId> partConf = new ArrayList<>();
        partConf.add(cluster.getLeader().getLeaderId());
        // part of peers conf, only contains leader peer
        rt.updateConfiguration(groupId, new Configuration(partConf));
        // fetch all conf
        final Status st = rt.refreshConfiguration(cliClientService, groupId, 10000);
        assertTrue(st.isOk());
        final Configuration newCnf = rt.getConfiguration(groupId);
        assertArrayEquals(new HashSet<>(cluster.getPeers()).toArray(), new HashSet<>(newCnf.getPeerSet()).toArray());
    }

    @Test
    public void testRefreshConfigurationFail() throws Exception {
        cluster.stopAll();
        final RouteTable rt = RouteTable.getInstance();
        rt.updateConfiguration(groupId, new Configuration(cluster.getPeers()));
        final Status st = rt.refreshConfiguration(cliClientService, groupId, 10000);
        assertFalse(st.isOk());
    }

    @Test
    public void testRefreshLeaderFail() throws TimeoutException, InterruptedException {
        cluster.stopAll();
        GrpcResponseFactory factory = new GrpcResponseFactory();
        final Message resp = factory.newResponse(CliRequests.GetLeaderResponse.getDefaultInstance(), new Status(-1,
            "err test"));
        final CompletableFuture<Message> respFuture = new CompletableFuture<>();
        respFuture.complete(resp);
        Mockito.when(this.mockCliClientService.connect(Mockito.any(Endpoint.class))).thenReturn(true);
        Mockito.when(
            this.mockCliClientService.getLeader(Mockito.any(Endpoint.class),
                Mockito.any(CliRequests.GetLeaderRequest.class), Mockito.any())).thenReturn(respFuture);
        final RouteTable rt = RouteTable.getInstance();
        rt.updateConfiguration(groupId, new Configuration(cluster.getPeers()));
        final Status status = rt.refreshLeader(this.mockCliClientService, groupId, 5000);
        assertFalse(status.isOk());
        assertEquals("err test, err test, err test", status.getErrorMsg());
    }

    @Test
    public void testRefreshConfigurationFail_2() throws TimeoutException, InterruptedException {
        cluster.stopAll();
        GrpcResponseFactory factory = new GrpcResponseFactory();
        final CliRequests.GetLeaderResponse resp = CliRequests.GetLeaderResponse.newBuilder()
            .setLeaderId("127.0.0.1:8001").build();
        final CompletableFuture<Message> getLeaderFuture = new CompletableFuture<>();
        getLeaderFuture.complete(resp);
        final Message getPeerResp = factory.newResponse(CliRequests.GetPeersResponse.getDefaultInstance(), new Status(
            -1, "refresh conf err test"));

        final CompletableFuture<Message> getPeerRespFuture = new CompletableFuture<>();
        getPeerRespFuture.complete(getPeerResp);
        Mockito.when(this.mockCliClientService.connect(Mockito.any(Endpoint.class))).thenReturn(true);
        Mockito.when(
            this.mockCliClientService.getLeader(Mockito.any(Endpoint.class),
                Mockito.any(CliRequests.GetLeaderRequest.class), Mockito.any())).thenReturn(getLeaderFuture);
        Mockito.when(
            this.mockCliClientService.getPeers(Mockito.any(Endpoint.class),
                Mockito.any(CliRequests.GetPeersRequest.class), Mockito.any())).thenReturn(getPeerRespFuture);
        final RouteTable rt = RouteTable.getInstance();
        rt.updateConfiguration(groupId, new Configuration(cluster.getPeers()));
        final Status status = rt.refreshConfiguration(this.mockCliClientService, groupId, 5000);
        assertFalse(status.isOk());
        assertEquals("refresh conf err test", status.getErrorMsg());
    }
}
