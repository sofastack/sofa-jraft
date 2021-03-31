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
package com.alipay.sofa.jraft.rhea.client.pd;

import java.util.List;
import java.util.Map;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.client.RegionRouteTable;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * Placement driver client
 *
 * @author jiachun.fjc
 */
public interface PlacementDriverClient extends Lifecycle<PlacementDriverOptions> {

    /**
     * Returns the cluster id.
     */
    long getClusterId();

    /**
     * Query the region by region id.
     */
    Region getRegionById(final long regionId);

    /**
     * Returns the region to which the key belongs.
     */
    Region findRegionByKey(final byte[] key, final boolean forceRefresh);

    /**
     * Returns the regions to which the keys belongs.
     */
    Map<Region, List<byte[]>> findRegionsByKeys(final List<byte[]> keys, final boolean forceRefresh);

    /**
     * Returns the regions to which the keys belongs.
     */
    Map<Region, List<KVEntry>> findRegionsByKvEntries(final List<KVEntry> kvEntries, final boolean forceRefresh);

    /**
     * Returns the regions to which the keys belongs.
     */
    Map<Region, List<CASEntry>> findRegionsByCASEntries(final List<CASEntry> casEntries, final boolean forceRefresh);

    /**
     * Returns the list of regions covered by startKey and endKey.
     */
    List<Region> findRegionsByKeyRange(final byte[] startKey, final byte[] endKey, final boolean forceRefresh);

    /**
     * Returns the startKey of next region.
     */
    byte[] findStartKeyOfNextRegion(final byte[] key, final boolean forceRefresh);

    /**
     * Returns the regionRouteTable instance.
     */
    RegionRouteTable getRegionRouteTable();

    /**
     * Returns the store metadata of the current instance's store.
     * Construct initial data based on the configuration file if
     * the data on {@link PlacementDriverClient} is empty.
     */
    Store getStoreMetadata(final StoreEngineOptions opts);

    /**
     * Get the specified region leader communication address.
     */
    Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis);

    /**
     * Get the specified region random peer communication address,
     * format: [ip:port]
     */
    Endpoint getLuckyPeer(final long regionId, final boolean forceRefresh, final long timeoutMillis,
                          final Endpoint unExpect);

    /**
     * Refresh the routing information of the specified region
     */
    void refreshRouteConfiguration(final long regionId);

    /**
     * Transfer leader to specified peer.
     */
    boolean transferLeader(final long regionId, final Peer peer, final boolean refreshConf);

    /**
     * Join the specified region group.
     */
    boolean addReplica(final long regionId, final Peer peer, final boolean refreshConf);

    /**
     * Depart from the specified region group.
     */
    boolean removeReplica(final long regionId, final Peer peer, final boolean refreshConf);

    /**
     * Returns raft cluster prefix id.
     */
    String getClusterName();

    /**
     * Get the placement driver server's leader communication address.
     */
    Endpoint getPdLeader(final boolean forceRefresh, final long timeoutMillis);

    /**
     * Returns the pd rpc service client.
     */
    PlacementDriverRpcService getPdRpcService();
}
