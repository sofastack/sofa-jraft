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

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.ClusterStatsManager;
import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.pipeline.event.RegionPingEvent;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.pipeline.Handler;
import com.alipay.sofa.jraft.rhea.util.pipeline.HandlerContext;
import com.alipay.sofa.jraft.rhea.util.pipeline.InboundHandlerAdapter;
import com.alipay.sofa.jraft.util.SPI;

/**
 * Range split judge, the reference indicator for splitting is the
 * region's approximate keys.
 *
 * @author jiachun.fjc
 */
@SPI(name = "splittingJudgeByApproximateKeys", priority = 50)
@Handler.Sharable
public class SplittingJudgeByApproximateKeysHandler extends InboundHandlerAdapter<RegionPingEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(SplittingJudgeByApproximateKeysHandler.class);

    @Override
    public void readMessage(final HandlerContext ctx, final RegionPingEvent event) throws Exception {
        if (event.isReady()) {
            return;
        }
        final MetadataStore metadataStore = event.getMetadataStore();
        final RegionHeartbeatRequest request = event.getMessage();
        final long clusterId = request.getClusterId();
        final ClusterStatsManager clusterStatsManager = ClusterStatsManager.getInstance(clusterId);
        clusterStatsManager.addOrUpdateRegionStats(request.getRegionStatsList());
        final Set<Long> stores = metadataStore.unsafeGetStoreIds(clusterId);
        if (stores == null || stores.isEmpty()) {
            return;
        }
        if (clusterStatsManager.regionSize() >= stores.size()) {
            // one store one region is perfect
            return;
        }
        final Pair<Region, RegionStats> modelWorker = clusterStatsManager.findModelWorkerRegion();
        if (!isSplitNeeded(request, modelWorker)) {
            return;
        }

        LOG.info("[Cluster: {}] model worker region is: {}.", clusterId, modelWorker);

        final Long newRegionId = metadataStore.createRegionId(clusterId);
        final Instruction.RangeSplit rangeSplit = new Instruction.RangeSplit();
        rangeSplit.setNewRegionId(newRegionId);
        final Instruction instruction = new Instruction();
        instruction.setRegion(modelWorker.getKey().copy());
        instruction.setRangeSplit(rangeSplit);
        event.addInstruction(instruction);
    }

    private boolean isSplitNeeded(final RegionHeartbeatRequest request, final Pair<Region, RegionStats> modelWorker) {
        if (modelWorker == null) {
            return false;
        }
        final long modelApproximateKeys = modelWorker.getValue().getApproximateKeys();
        if (request.getLeastKeysOnSplit() > modelApproximateKeys) {
            return false;
        }
        final Region modelRegion = modelWorker.getKey();
        final List<Pair<Region, RegionStats>> regionStatsList = request.getRegionStatsList();
        for (final Pair<Region, RegionStats> p : regionStatsList) {
            if (modelRegion.equals(p.getKey())) {
                return true;
            }
        }
        return false;
    }
}
