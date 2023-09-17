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
package com.alipay.sofa.jraft.test.atomic.server;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.test.atomic.KeyNotFoundException;
import com.alipay.sofa.jraft.test.atomic.command.BooleanCommand;
import com.alipay.sofa.jraft.test.atomic.command.ValueCommand;
import com.alipay.sofa.jraft.test.atomic.server.processor.GetCommandProcessor;
import com.alipay.sofa.jraft.util.Bits;
import com.codahale.metrics.ConsoleReporter;

/**
 * Atomic range node in a raft group.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 4:51:02 PM
 */
public class AtomicRangeGroup {

    final static Logger              LOG       = LoggerFactory.getLogger(AtomicRangeGroup.class);

    private final RaftGroupService   raftGroupService;
    private final Node               node;
    private final AtomicStateMachine fsm;

    private final long               minSlot;                                                    //inclusion
    private final long               maxSlot;                                                    //exclusion
    private final AtomicInteger      requestId = new AtomicInteger(0);

    public long getMinSlot() {
        return this.minSlot;
    }

    public long getMaxSlot() {
        return this.maxSlot;
    }

    public AtomicRangeGroup(String dataPath, String groupId, PeerId serverId, long minSlot, long maxSlot,
                            NodeOptions nodeOptions, RpcServer rpcServer) throws IOException {
        // Init file path
        FileUtils.forceMkdir(new File(dataPath));
        this.minSlot = minSlot;
        this.maxSlot = maxSlot;

        // Init statemachine
        this.fsm = new AtomicStateMachine();

        // Set statemachine to bootstrap options
        nodeOptions.setFsm(this.fsm);
        nodeOptions.setEnableMetrics(true);
        nodeOptions.getRaftOptions().setReplicatorPipeline(true);
        nodeOptions.getRaftOptions().setSync(true);
        nodeOptions.getRaftOptions().setReadOnlyOptions(ReadOnlyOption.ReadOnlySafe);

        // Set the data path
        // Log, required
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // Metadata, required
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        // Snapshot, not required, but recommend
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // Init raft group service framework
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer);
        // Startup node
        this.node = this.raftGroupService.start();

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(node.getNodeMetrics().getMetricRegistry())
            .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(60, TimeUnit.SECONDS);

    }

    public void readFromQuorum(final String key, RpcContext asyncContext) {
        final byte[] reqContext = new byte[4];
        Bits.putInt(reqContext, 0, requestId.incrementAndGet());
        this.node.readIndex(reqContext, new ReadIndexClosure() {

            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    try {
                        asyncContext.sendResponse(new ValueCommand(fsm.getValue(key)));
                    } catch (final KeyNotFoundException e) {
                        asyncContext.sendResponse(GetCommandProcessor.createKeyNotFoundResponse());
                    }
                } else {
                    asyncContext.sendResponse(new BooleanCommand(false, status.getErrorMsg()));
                }
            }
        });
    }

    public AtomicStateMachine getFsm() {
        return this.fsm;
    }

    public Node getNode() {
        return this.node;
    }

    public RaftGroupService RaftGroupService() {
        return this.raftGroupService;
    }

    /**
     * Redirect request to new leader
     * @return
     */
    public BooleanCommand redirect() {
        final BooleanCommand response = new BooleanCommand();
        response.setSuccess(false);
        response.setErrorMsg("Not leader");
        if (node != null) {
            final PeerId leader = node.getLeaderId();
            if (leader != null) {
                response.setRedirect(leader.toString());
            }
        }

        return response;
    }

    public static AtomicRangeGroup start(StartupConf conf, RpcServer rpcServer) throws IOException {

        final NodeOptions nodeOptions = new NodeOptions();
        // Set election timeout to 1 second
        nodeOptions.setElectionTimeoutMs(1000);
        // Close cli service
        nodeOptions.setDisableCli(false);
        // A snapshot saving would be triggered every 30 seconds
        // nodeOptions.setSnapshotIntervalSecs(30);
        // Parsing Options
        final PeerId serverId = new PeerId();
        if (!serverId.parse(conf.getServerAddress())) {
            throw new IllegalArgumentException("Fail to parse serverId:" + conf.getServerAddress());
        }
        final Configuration initConf = new Configuration();
        if (!initConf.parse(conf.getConf())) {
            throw new IllegalArgumentException("Fail to parse initConf:" + conf.getConf());
        }
        // Set the initial cluster configuration
        nodeOptions.setInitialConf(initConf);
        // Startup node
        final AtomicRangeGroup node = new AtomicRangeGroup(conf.getDataPath(), conf.getGroupId(), serverId,
            conf.getMinSlot(), conf.getMaxSlot(), nodeOptions, rpcServer);
        LOG.info("Started range node[{}-{}] at port:{}", conf.getMinSlot(), conf.getMaxSlot(), node.getNode()
            .getNodeId().getPeerId().getPort());
        return node;
    }
}
