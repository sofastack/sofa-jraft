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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.metadata.TimeInterval;
import com.alipay.sofa.jraft.rhea.pipeline.event.RegionPingEvent;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.pipeline.Handler;
import com.alipay.sofa.jraft.rhea.util.pipeline.HandlerContext;
import com.alipay.sofa.jraft.rhea.util.pipeline.InboundHandlerAdapter;
import com.alipay.sofa.jraft.util.SPI;

/**
 *
 * @author jiachun.fjc
 */
@SPI(name = "regionStatsValidator", priority = 90)
@Handler.Sharable
public class RegionStatsValidator extends InboundHandlerAdapter<RegionPingEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(RegionStatsValidator.class);

    @Override
    public void readMessage(final HandlerContext ctx, final RegionPingEvent event) throws Exception {
        final MetadataStore metadataStore = event.getMetadataStore();
        final RegionHeartbeatRequest request = event.getMessage();
        final List<Pair<Region, RegionStats>> regionStatsList = request.getRegionStatsList();
        if (regionStatsList == null || regionStatsList.isEmpty()) {
            LOG.error("Empty [RegionStatsList] by event: {}.", event);
            throw Errors.INVALID_REGION_STATS.exception();
        }
        for (final Pair<Region, RegionStats> pair : regionStatsList) {
            final Region region = pair.getKey();
            if (region == null) {
                LOG.error("Empty [Region] by event: {}.", event);
                throw Errors.INVALID_REGION_STATS.exception();
            }
            final RegionEpoch regionEpoch = region.getRegionEpoch();
            if (regionEpoch == null) {
                LOG.error("Empty [RegionEpoch] by event: {}.", event);
                throw Errors.INVALID_REGION_STATS.exception();
            }
            final RegionStats regionStats = pair.getValue();
            if (regionStats == null) {
                LOG.error("Empty [RegionStats] by event: {}.", event);
                throw Errors.INVALID_REGION_STATS.exception();
            }
            final Pair<Region, RegionStats> currentRegionInfo = metadataStore.getRegionStats(request.getClusterId(),
                region);
            if (currentRegionInfo == null) {
                return; // new data
            }
            final Region currentRegion = currentRegionInfo.getKey();
            if (regionEpoch.compareTo(currentRegion.getRegionEpoch()) < 0) {
                LOG.error("The region epoch is out of date: {}.", event);
                throw Errors.REGION_HEARTBEAT_OUT_OF_DATE.exception();
            }
            final TimeInterval interval = regionStats.getInterval();
            if (interval == null) {
                LOG.error("Empty [TimeInterval] by event: {}.", event);
                throw Errors.INVALID_REGION_STATS.exception();
            }
            final TimeInterval currentInterval = currentRegionInfo.getValue().getInterval();
            if (interval.getEndTimestamp() < currentInterval.getEndTimestamp()) {
                LOG.error("The [TimeInterval] is out of date: {}.", event);
                throw Errors.REGION_HEARTBEAT_OUT_OF_DATE.exception();
            }
        }
    }
}
