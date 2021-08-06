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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.client.RegionRouteTable;
import com.alipay.sofa.jraft.rhea.client.RoundRobinLoadBalancer;
import com.alipay.sofa.jraft.rhea.errors.RouteTableException;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.options.configured.RpcOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.rpc.CliClientService;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.AbstractClientService;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;

/**
 *
 * @author jiachun.fjc
 */
public abstract class AbstractPlacementDriverClient implements PlacementDriverClient {

    private static final Logger         LOG                      = LoggerFactory
                                                                     .getLogger(AbstractPlacementDriverClient.class);

    private static final long           AT_LEAST_REQUIRED_MILLIS = TimeUnit.SECONDS.toMillis(1);

    protected final RegionRouteTable    regionRouteTable         = new RegionRouteTable();
    protected final long                clusterId;
    protected final String              clusterName;

    protected CliService                cliService;
    protected CliClientService          cliClientService;
    protected RpcClient                 rpcClient;
    protected PlacementDriverRpcService pdRpcService;

    protected AbstractPlacementDriverClient(long clusterId, String clusterName) {
        this.clusterId = clusterId;
        this.clusterName = clusterName;
    }

    @Override
    public synchronized boolean init(final PlacementDriverOptions opts) {
        initCli(opts.getCliOptions());
        this.pdRpcService = new DefaultPlacementDriverRpcService(this);
        RpcOptions rpcOpts = opts.getPdRpcOptions();
        if (rpcOpts == null) {
            rpcOpts = RpcOptionsConfigured.newDefaultConfig();
            rpcOpts.setCallbackExecutorCorePoolSize(0);
            rpcOpts.setCallbackExecutorMaximumPoolSize(0);
        }
        if (!this.pdRpcService.init(rpcOpts)) {
            LOG.error("Fail to init [PlacementDriverRpcService].");
            return false;
        }
        // region route table
        final List<RegionRouteTableOptions> regionRouteTableOptionsList = opts.getRegionRouteTableOptionsList();
        if (regionRouteTableOptionsList != null) {
            final String initialServerList = opts.getInitialServerList();
            for (final RegionRouteTableOptions regionRouteTableOpts : regionRouteTableOptionsList) {
                if (Strings.isBlank(regionRouteTableOpts.getInitialServerList())) {
                    // if blank, extends parent's value
                    regionRouteTableOpts.setInitialServerList(initialServerList);
                }
                initRouteTableByRegion(regionRouteTableOpts);
            }
        }
        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.cliService != null) {
            this.cliService.shutdown();
        }
        if (this.pdRpcService != null) {
            this.pdRpcService.shutdown();
        }
    }

    @Override
    public long getClusterId() {
        return clusterId;
    }

    @Override
    public Region getRegionById(final long regionId) {
        return this.regionRouteTable.getRegionById(regionId);
    }

    @Override
    public Region findRegionByKey(final byte[] key, final boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findRegionByKey(key);
    }

    @Override
    public Map<Region, List<byte[]>> findRegionsByKeys(final List<byte[]> keys, final boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findRegionsByKeys(keys);
    }

    @Override
    public Map<Region, List<KVEntry>> findRegionsByKvEntries(final List<KVEntry> kvEntries, final boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findRegionsByKvEntries(kvEntries);
    }

    @Override
    public Map<Region, List<CASEntry>> findRegionsByCASEntries(List<CASEntry> casEntries, boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findRegionsByCASEntries(casEntries);
    }

    @Override
    public List<Region> findRegionsByKeyRange(final byte[] startKey, final byte[] endKey, final boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findRegionsByKeyRange(startKey, endKey);
    }

    @Override
    public byte[] findStartKeyOfNextRegion(final byte[] key, final boolean forceRefresh) {
        if (forceRefresh) {
            refreshRouteTable();
        }
        return this.regionRouteTable.findStartKeyOfNextRegion(key);
    }

    @Override
    public RegionRouteTable getRegionRouteTable() {
        return regionRouteTable;
    }

    @Override
    public boolean transferLeader(final long regionId, final Peer peer, final boolean refreshConf) {
        Requires.requireNonNull(peer, "peer");
        Requires.requireNonNull(peer.getEndpoint(), "peer.endpoint");
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final Configuration conf = RouteTable.getInstance().getConfiguration(raftGroupId);
        final Status status = this.cliService.transferLeader(raftGroupId, conf, JRaftHelper.toJRaftPeerId(peer));
        if (status.isOk()) {
            if (refreshConf) {
                refreshRouteConfiguration(regionId);
            }
            return true;
        }
        LOG.error("Fail to [transferLeader], [regionId: {}, peer: {}], status: {}.", regionId, peer, status);
        return false;
    }

    @Override
    public boolean addReplica(final long regionId, final Peer peer, final boolean refreshConf) {
        Requires.requireNonNull(peer, "peer");
        Requires.requireNonNull(peer.getEndpoint(), "peer.endpoint");
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final Configuration conf = RouteTable.getInstance().getConfiguration(raftGroupId);
        final Status status = this.cliService.addPeer(raftGroupId, conf, JRaftHelper.toJRaftPeerId(peer));
        if (status.isOk()) {
            if (refreshConf) {
                refreshRouteConfiguration(regionId);
            }
            return true;
        }
        LOG.error("Fail to [addReplica], [regionId: {}, peer: {}], status: {}.", regionId, peer, status);
        return false;
    }

    @Override
    public boolean removeReplica(final long regionId, final Peer peer, final boolean refreshConf) {
        Requires.requireNonNull(peer, "peer");
        Requires.requireNonNull(peer.getEndpoint(), "peer.endpoint");
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final Configuration conf = RouteTable.getInstance().getConfiguration(raftGroupId);
        final Status status = this.cliService.removePeer(raftGroupId, conf, JRaftHelper.toJRaftPeerId(peer));
        if (status.isOk()) {
            if (refreshConf) {
                refreshRouteConfiguration(regionId);
            }
            return true;
        }
        LOG.error("Fail to [removeReplica], [regionId: {}, peer: {}], status: {}.", regionId, peer, status);
        return false;
    }

    @Override
    public Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis) {
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        PeerId leader = getLeader(raftGroupId, forceRefresh, timeoutMillis);
        if (leader == null && !forceRefresh) {
            // Could not found leader from cache, try again and force refresh cache
            leader = getLeader(raftGroupId, true, timeoutMillis);
        }
        if (leader == null) {
            throw new RouteTableException("no leader in group: " + raftGroupId);
        }
        return leader.getEndpoint();
    }

    protected PeerId getLeader(final String raftGroupId, final boolean forceRefresh, final long timeoutMillis) {
        final RouteTable routeTable = RouteTable.getInstance();
        if (forceRefresh) {
            final long deadline = System.currentTimeMillis() + timeoutMillis;
            final StringBuilder error = new StringBuilder();
            // A newly launched raft group may not have been successful in the election,
            // or in the 'leader-transfer' state, it needs to be re-tried
            Throwable lastCause = null;
            for (;;) {
                try {
                    final Status st = routeTable.refreshLeader(this.cliClientService, raftGroupId, 2000);
                    if (st.isOk()) {
                        break;
                    }
                    error.append(st.toString());
                } catch (final InterruptedException e) {
                    ThrowUtil.throwException(e);
                } catch (final Throwable t) {
                    lastCause = t;
                    error.append(t.getMessage());
                }
                if (System.currentTimeMillis() < deadline) {
                    LOG.debug("Fail to find leader, retry again, {}.", error);
                    error.append(", ");
                    try {
                        Thread.sleep(10);
                    } catch (final InterruptedException e) {
                        ThrowUtil.throwException(e);
                    }
                } else {
                    throw lastCause != null ? new RouteTableException(error.toString(), lastCause)
                        : new RouteTableException(error.toString());
                }
            }

            // we need refresh configuration for membership change
            final long leftTime = deadline - System.currentTimeMillis();
            if (leftTime > AT_LEAST_REQUIRED_MILLIS) {
                try {
                    RouteTable.getInstance().refreshConfiguration(this.cliClientService, raftGroupId, (int) leftTime);
                } catch (final InterruptedException e) {
                    ThrowUtil.throwException(e);
                } catch (final TimeoutException ignored) {
                }
            }
        }
        return routeTable.selectLeader(raftGroupId);
    }

    @Override
    public Endpoint getLuckyPeer(final long regionId, final boolean forceRefresh, final long timeoutMillis,
                                 final Endpoint unExpect) {
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final RouteTable routeTable = RouteTable.getInstance();
        if (forceRefresh) {
            final long deadline = System.currentTimeMillis() + timeoutMillis;
            final StringBuilder error = new StringBuilder();
            // A newly launched raft group may not have been successful in the election,
            // or in the 'leader-transfer' state, it needs to be re-tried
            for (;;) {
                try {
                    final Status st = routeTable.refreshConfiguration(this.cliClientService, raftGroupId, 5000);
                    if (st.isOk()) {
                        break;
                    }
                    error.append(st.toString());
                } catch (final InterruptedException e) {
                    ThrowUtil.throwException(e);
                } catch (final TimeoutException e) {
                    error.append(e.getMessage());
                }
                if (System.currentTimeMillis() < deadline) {
                    LOG.debug("Fail to get peers, retry again, {}.", error);
                    error.append(", ");
                    try {
                        Thread.sleep(5);
                    } catch (final InterruptedException e) {
                        ThrowUtil.throwException(e);
                    }
                } else {
                    throw new RouteTableException(error.toString());
                }
            }
        }
        final Configuration configs = routeTable.getConfiguration(raftGroupId);
        if (configs == null) {
            throw new RouteTableException("empty configs in group: " + raftGroupId);
        }
        final List<PeerId> peerList = configs.getPeers();
        if (peerList == null || peerList.isEmpty()) {
            throw new RouteTableException("empty peers in group: " + raftGroupId);
        }
        final int size = peerList.size();
        if (size == 1) {
            return peerList.get(0).getEndpoint();
        }
        final RoundRobinLoadBalancer balancer = RoundRobinLoadBalancer.getInstance(regionId);
        for (int i = 0; i < size; i++) {
            final PeerId candidate = balancer.select(peerList);
            final Endpoint luckyOne = candidate.getEndpoint();
            if (!luckyOne.equals(unExpect)) {
                return luckyOne;
            }
        }
        throw new RouteTableException("have no choice in group(peers): " + raftGroupId);
    }

    @Override
    public void refreshRouteConfiguration(final long regionId) {
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        try {
            getLeader(raftGroupId, true, 5000);
            RouteTable.getInstance().refreshConfiguration(this.cliClientService, raftGroupId, 5000);
        } catch (final Exception e) {
            LOG.error("Fail to refresh route configuration for {}, {}.", regionId, StackTraceUtil.stackTrace(e));
        }
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public PlacementDriverRpcService getPdRpcService() {
        return pdRpcService;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    protected void initRouteTableByRegion(final RegionRouteTableOptions opts) {
        final long regionId = Requires.requireNonNull(opts.getRegionId(), "opts.regionId");
        final byte[] startKey = opts.getStartKeyBytes();
        final byte[] endKey = opts.getEndKeyBytes();
        final String initialServerList = opts.getInitialServerList();
        final Region region = new Region();
        final Configuration conf = new Configuration();
        // region
        region.setId(regionId);
        region.setStartKey(startKey);
        region.setEndKey(endKey);
        region.setRegionEpoch(new RegionEpoch(-1, -1));
        // peers
        Requires.requireTrue(Strings.isNotBlank(initialServerList), "opts.initialServerList is blank");
        conf.parse(initialServerList);
        region.setPeers(JRaftHelper.toPeerList(conf.listPeers()));
        // update raft route table
        RouteTable.getInstance().updateConfiguration(JRaftHelper.getJRaftGroupId(clusterName, regionId), conf);
        this.regionRouteTable.addOrUpdateRegion(region);
    }

    protected Region getLocalRegionMetadata(final RegionEngineOptions opts) {
        final long regionId = Requires.requireNonNull(opts.getRegionId(), "opts.regionId");
        Requires.requireTrue(regionId >= Region.MIN_ID_WITH_MANUAL_CONF, "opts.regionId must >= "
                                                                         + Region.MIN_ID_WITH_MANUAL_CONF);
        Requires.requireTrue(regionId < Region.MAX_ID_WITH_MANUAL_CONF, "opts.regionId must < "
                                                                        + Region.MAX_ID_WITH_MANUAL_CONF);
        final byte[] startKey = opts.getStartKeyBytes();
        final byte[] endKey = opts.getEndKeyBytes();
        final String initialServerList = opts.getInitialServerList();
        final Region region = new Region();
        final Configuration conf = new Configuration();
        // region
        region.setId(regionId);
        region.setStartKey(startKey);
        region.setEndKey(endKey);
        region.setRegionEpoch(new RegionEpoch(-1, -1));
        // peers
        Requires.requireTrue(Strings.isNotBlank(initialServerList), "opts.initialServerList is blank");
        conf.parse(initialServerList);
        region.setPeers(JRaftHelper.toPeerList(conf.listPeers()));
        this.regionRouteTable.addOrUpdateRegion(region);
        return region;
    }

    protected void initCli(CliOptions cliOpts) {
        if (cliOpts == null) {
            cliOpts = new CliOptions();
            cliOpts.setTimeoutMs(5000);
            cliOpts.setMaxRetry(3);
        }
        this.cliService = RaftServiceFactory.createAndInitCliService(cliOpts);
        this.cliClientService = ((CliServiceImpl) this.cliService).getCliClientService();
        Requires.requireNonNull(this.cliClientService, "cliClientService");
        this.rpcClient = ((AbstractClientService) this.cliClientService).getRpcClient();
    }

    protected abstract void refreshRouteTable();
}
