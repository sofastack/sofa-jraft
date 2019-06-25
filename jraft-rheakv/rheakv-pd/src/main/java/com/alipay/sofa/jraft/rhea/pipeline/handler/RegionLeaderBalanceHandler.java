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
package com.alipay.sofa.jraft.rhea.pipeline.handler;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.ClusterStatsManager;
import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.metadata.StoreStats;
import com.alipay.sofa.jraft.rhea.pipeline.event.RegionPingEvent;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.pipeline.Handler;
import com.alipay.sofa.jraft.rhea.util.pipeline.HandlerContext;
import com.alipay.sofa.jraft.rhea.util.pipeline.InboundHandlerAdapter;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.SPI;

/**
 * Trying to balance the number of leaders in each store.
 *
 * @author jiachun.fjc
 */
@SPI(name = "regionLeaderBalance", priority = 60)
@Handler.Sharable
public class RegionLeaderBalanceHandler extends InboundHandlerAdapter<RegionPingEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(RegionLeaderBalanceHandler.class);

    @Override
    public void readMessage(final HandlerContext ctx, final RegionPingEvent event) throws Exception {
        if (event.isReady()) {
            return;
        }
        final MetadataStore metadataStore = event.getMetadataStore();
        final RegionHeartbeatRequest request = event.getMessage();
        final long clusterId = request.getClusterId();
        final long storeId = request.getStoreId();
        final ClusterStatsManager clusterStatsManager = ClusterStatsManager.getInstance(clusterId);
        final List<Pair<Region, RegionStats>> regionStatsList = request.getRegionStatsList();
        for (final Pair<Region, RegionStats> stats : regionStatsList) {
            final Region region = stats.getKey();
            clusterStatsManager.addOrUpdateLeader(storeId, region.getId());
        }

        // check if the modelWorker
        final Pair<Set<Long>, Integer> modelWorkers = clusterStatsManager.findModelWorkerStores(1);
        final Set<Long> modelWorkerStoreIds = modelWorkers.getKey();
        final int modelWorkerLeaders = modelWorkers.getValue();
        if (!modelWorkerStoreIds.contains(storeId)) {
            return;
        }

        LOG.info("[Cluster: {}] model worker stores is: {}, it has {} leaders.", clusterId, modelWorkerStoreIds, modelWorkerLeaders);

        for (final Pair<Region, RegionStats> pair : regionStatsList) {
            final Region region = pair.getKey();
            final List<Peer> peers = region.getPeers();
            if (peers == null) {
                continue;
            }
            final List<Endpoint> endpoints = Lists.transform(peers, Peer::getEndpoint);
            final Map<Long, Endpoint> storeIds = metadataStore.unsafeGetStoreIdsByEndpoints(clusterId, endpoints);
            // find lazyWorkers
            final List<Pair<Long, Integer>> lazyWorkers = clusterStatsManager.findLazyWorkerStores(storeIds.keySet());
            if (lazyWorkers.isEmpty()) {
                return;
            }
            for (int i = lazyWorkers.size() - 1; i >= 0; i--) {
                final Pair<Long, Integer> worker = lazyWorkers.get(i);
                if (modelWorkerLeaders - worker.getValue() <= 1) { // no need to transfer
                    lazyWorkers.remove(i);
                }
            }
            if (lazyWorkers.isEmpty()) {
                continue;
            }
            final Pair<Long, Integer> laziestWorker = tryToFindLaziestWorker(clusterId, metadataStore, lazyWorkers);
            if (laziestWorker == null) {
                continue;
            }
            final Long lazyWorkerStoreId = laziestWorker.getKey();
            LOG.info("[Cluster: {}], lazy worker store is: {}, it has {} leaders.", clusterId, lazyWorkerStoreId,
                    laziestWorker.getValue());
            final Instruction.TransferLeader transferLeader = new Instruction.TransferLeader();
            transferLeader.setMoveToStoreId(lazyWorkerStoreId);
            transferLeader.setMoveToEndpoint(storeIds.get(lazyWorkerStoreId));
            final Instruction instruction = new Instruction();
            instruction.setRegion(region.copy());
            instruction.setTransferLeader(transferLeader);
            event.addInstruction(instruction);
            LOG.info("[Cluster: {}], send 'instruction.transferLeader': {} to region: {}.", clusterId, instruction, region);
            break; // Only do one thing at a time
        }
    }

    private Pair<Long, Integer> tryToFindLaziestWorker(final long clusterId, final MetadataStore metadataStore,
                                                       final List<Pair<Long, Integer>> lazyWorkers) {
        final List<Pair<Pair<Long, Integer>, StoreStats>> storeStatsList = Lists.newArrayList();
        for (final Pair<Long, Integer> worker : lazyWorkers) {
            final StoreStats stats = metadataStore.getStoreStats(clusterId, worker.getKey());
            if (stats != null) {
                // TODO check timeInterval
                storeStatsList.add(Pair.of(worker, stats));
            }
        }
        if (storeStatsList.isEmpty()) {
            return null;
        }
        if (storeStatsList.size() == 1) {
            return storeStatsList.get(0).getKey();
        }
        final Pair<Pair<Long, Integer>, StoreStats> min = Collections.min(storeStatsList, (o1, o2) -> {
            final StoreStats s1 = o1.getValue();
            final StoreStats s2 = o2.getValue();
            int val = Boolean.compare(s1.isBusy(), s2.isBusy());
            if (val != 0) {
                return val;
            }
            val = Integer.compare(s1.getRegionCount(), s2.getRegionCount());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getBytesWritten(), s2.getBytesWritten());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getBytesRead(), s2.getBytesRead());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getKeysWritten(), s2.getKeysWritten());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getKeysRead(), s2.getKeysRead());
            if (val != 0) {
                return val;
            }
            return Long.compare(-s1.getAvailable(), -s2.getAvailable());
        });
        return min.getKey();
    }
}
