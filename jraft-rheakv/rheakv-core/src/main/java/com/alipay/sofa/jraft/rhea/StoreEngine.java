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
package com.alipay.sofa.jraft.rhea;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rhea.client.pd.HeartbeatSender;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.pd.RemotePlacementDriverClient;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.HeartbeatOptions;
import com.alipay.sofa.jraft.rhea.options.MemoryDBOptions;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.BatchRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVClosureAdapter;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.MemoryRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.rhea.util.Constants;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.NetUtil;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolMetricRegistry;
import com.alipay.sofa.jraft.util.Utils;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;

/**
 * Storage engine, there is only one instance in a node,
 * containing one or more {@link RegionEngine}.
 *
 * @author jiachun.fjc
 */
public class StoreEngine implements Lifecycle<StoreEngineOptions>, Describer {

    private static final Logger                        LOG                  = LoggerFactory
                                                                                .getLogger(StoreEngine.class);

    static {
        ExtSerializerSupports.init();
    }

    private final ConcurrentMap<Long, RegionKVService> regionKVServiceTable = Maps.newConcurrentMapLong();
    private final ConcurrentMap<Long, RegionEngine>    regionEngineTable    = Maps.newConcurrentMapLong();
    private final StateListenerContainer<Long>         stateListenerContainer;
    private final PlacementDriverClient                pdClient;
    private final long                                 clusterId;

    private Long                                       storeId;
    private final AtomicBoolean                        splitting            = new AtomicBoolean(false);
    // When the store is started (unix timestamp in milliseconds)
    private long                                       startTime            = System.currentTimeMillis();
    private File                                       dbPath;
    private RpcServer                                  rpcServer;
    private BatchRawKVStore<?>                         rawKVStore;
    private HeartbeatSender                            heartbeatSender;
    private StoreEngineOptions                         storeOpts;

    // Shared executor services
    private ExecutorService                            readIndexExecutor;
    private ExecutorService                            raftStateTrigger;
    private ExecutorService                            snapshotExecutor;
    private ExecutorService                            cliRpcExecutor;
    private ExecutorService                            raftRpcExecutor;
    private ExecutorService                            kvRpcExecutor;

    private ScheduledExecutorService                   metricsScheduler;
    private ScheduledReporter                          kvMetricsReporter;
    private ScheduledReporter                          threadPoolMetricsReporter;

    private boolean                                    started;

    public StoreEngine(PlacementDriverClient pdClient, StateListenerContainer<Long> stateListenerContainer) {
        this.pdClient = Requires.requireNonNull(pdClient, "pdClient");
        this.clusterId = pdClient.getClusterId();
        this.stateListenerContainer = Requires.requireNonNull(stateListenerContainer, "stateListenerContainer");
    }

    @Override
    public synchronized boolean init(final StoreEngineOptions opts) {
        if (this.started) {
            LOG.info("[StoreEngine] already started.");
            return true;
        }

        DescriberManager.getInstance().addDescriber(this);

        this.storeOpts = Requires.requireNonNull(opts, "opts");
        Endpoint serverAddress = Requires.requireNonNull(opts.getServerAddress(), "opts.serverAddress");
        final int port = serverAddress.getPort();
        final String ip = serverAddress.getIp();
        if (ip == null || Utils.IP_ANY.equals(ip)) {
            serverAddress = new Endpoint(NetUtil.getLocalCanonicalHostName(), port);
            opts.setServerAddress(serverAddress);
        }
        final long metricsReportPeriod = opts.getMetricsReportPeriod();
        // init region options
        List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        if (rOptsList == null || rOptsList.isEmpty()) {
            // -1 region
            final RegionEngineOptions rOpts = new RegionEngineOptions();
            rOpts.setRegionId(Constants.DEFAULT_REGION_ID);
            rOptsList = Lists.newArrayList();
            rOptsList.add(rOpts);
            opts.setRegionEngineOptionsList(rOptsList);
        }
        final String clusterName = this.pdClient.getClusterName();
        for (final RegionEngineOptions rOpts : rOptsList) {
            rOpts.setRaftGroupId(JRaftHelper.getJRaftGroupId(clusterName, rOpts.getRegionId()));
            rOpts.setServerAddress(serverAddress);
            if (Strings.isBlank(rOpts.getInitialServerList())) {
                // if blank, extends parent's value
                rOpts.setInitialServerList(opts.getInitialServerList());
            }
            if (rOpts.getNodeOptions() == null) {
                // copy common node options
                rOpts.setNodeOptions(opts.getCommonNodeOptions() == null ? new NodeOptions() : opts
                    .getCommonNodeOptions().copy());
            }
            if (rOpts.getMetricsReportPeriod() <= 0 && metricsReportPeriod > 0) {
                // extends store opts
                rOpts.setMetricsReportPeriod(metricsReportPeriod);
            }
        }
        // init store
        final Store store = this.pdClient.getStoreMetadata(opts);
        if (store == null || store.getRegions() == null || store.getRegions().isEmpty()) {
            LOG.error("Empty store metadata: {}.", store);
            return false;
        }
        this.storeId = store.getId();
        // init executors
        if (this.readIndexExecutor == null) {
            this.readIndexExecutor = StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
        }
        if (this.raftStateTrigger == null) {
            this.raftStateTrigger = StoreEngineHelper.createRaftStateTrigger(opts.getLeaderStateTriggerCoreThreads());
        }
        if (this.snapshotExecutor == null) {
            this.snapshotExecutor = StoreEngineHelper.createSnapshotExecutor(opts.getSnapshotCoreThreads(),
                opts.getSnapshotMaxThreads());
        }
        // init rpc executors
        final boolean useSharedRpcExecutor = opts.isUseSharedRpcExecutor();
        if (!useSharedRpcExecutor) {
            if (this.cliRpcExecutor == null) {
                this.cliRpcExecutor = StoreEngineHelper.createCliRpcExecutor(opts.getCliRpcCoreThreads());
            }
            if (this.raftRpcExecutor == null) {
                this.raftRpcExecutor = StoreEngineHelper.createRaftRpcExecutor(opts.getRaftRpcCoreThreads());
            }
            if (this.kvRpcExecutor == null) {
                this.kvRpcExecutor = StoreEngineHelper.createKvRpcExecutor(opts.getKvRpcCoreThreads());
            }
        }
        // init metrics
        startMetricReporters(metricsReportPeriod);
        // init rpc server
        this.rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverAddress, this.raftRpcExecutor,
            this.cliRpcExecutor);
        StoreEngineHelper.addKvStoreRequestProcessor(this.rpcServer, this);
        if (!this.rpcServer.init(null)) {
            LOG.error("Fail to init [RpcServer].");
            return false;
        }
        // init db store
        if (!initRawKVStore(opts)) {
            return false;
        }
        if (this.rawKVStore instanceof Describer) {
            DescriberManager.getInstance().addDescriber((Describer) this.rawKVStore);
        }
        // init all region engine
        if (!initAllRegionEngine(opts, store)) {
            LOG.error("Fail to init all [RegionEngine].");
            return false;
        }
        // heartbeat sender
        if (this.pdClient instanceof RemotePlacementDriverClient) {
            HeartbeatOptions heartbeatOpts = opts.getHeartbeatOptions();
            if (heartbeatOpts == null) {
                heartbeatOpts = new HeartbeatOptions();
            }
            this.heartbeatSender = new HeartbeatSender(this);
            if (!this.heartbeatSender.init(heartbeatOpts)) {
                LOG.error("Fail to init [HeartbeatSender].");
                return false;
            }
        }
        this.startTime = System.currentTimeMillis();
        LOG.info("[StoreEngine] start successfully: {}.", this);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.rpcServer != null) {
            this.rpcServer.shutdown();
        }
        if (!this.regionEngineTable.isEmpty()) {
            for (final RegionEngine engine : this.regionEngineTable.values()) {
                engine.shutdown();
            }
            this.regionEngineTable.clear();
        }
        if (this.rawKVStore != null) {
            this.rawKVStore.shutdown();
        }
        if (this.heartbeatSender != null) {
            this.heartbeatSender.shutdown();
        }
        this.regionKVServiceTable.clear();
        if (this.kvMetricsReporter != null) {
            this.kvMetricsReporter.stop();
        }
        if (this.threadPoolMetricsReporter != null) {
            this.threadPoolMetricsReporter.stop();
        }
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.readIndexExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.raftStateTrigger);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.snapshotExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.cliRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.raftRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.kvRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.metricsScheduler);
        this.started = false;
        LOG.info("[StoreEngine] shutdown successfully.");
    }

    public PlacementDriverClient getPlacementDriverClient() {
        return pdClient;
    }

    public long getClusterId() {
        return clusterId;
    }

    public Long getStoreId() {
        return storeId;
    }

    public StoreEngineOptions getStoreOpts() {
        return storeOpts;
    }

    public long getStartTime() {
        return startTime;
    }

    public RpcServer getRpcServer() {
        return rpcServer;
    }

    public BatchRawKVStore<?> getRawKVStore() {
        return rawKVStore;
    }

    public RegionKVService getRegionKVService(final long regionId) {
        return this.regionKVServiceTable.get(regionId);
    }

    public long getTotalSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return this.dbPath.getTotalSpace();
    }

    public long getUsableSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return this.dbPath.getUsableSpace();
    }

    public long getStoreUsedSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return FileUtils.sizeOf(this.dbPath);
    }

    public Endpoint getSelfEndpoint() {
        return this.storeOpts == null ? null : this.storeOpts.getServerAddress();
    }

    public RegionEngine getRegionEngine(final long regionId) {
        return this.regionEngineTable.get(regionId);
    }

    public List<RegionEngine> getAllRegionEngines() {
        return Lists.newArrayList(this.regionEngineTable.values());
    }

    public ExecutorService getReadIndexExecutor() {
        return readIndexExecutor;
    }

    public void setReadIndexExecutor(ExecutorService readIndexExecutor) {
        this.readIndexExecutor = readIndexExecutor;
    }

    public ExecutorService getRaftStateTrigger() {
        return raftStateTrigger;
    }

    public void setRaftStateTrigger(ExecutorService raftStateTrigger) {
        this.raftStateTrigger = raftStateTrigger;
    }

    public ExecutorService getSnapshotExecutor() {
        return snapshotExecutor;
    }

    public void setSnapshotExecutor(ExecutorService snapshotExecutor) {
        this.snapshotExecutor = snapshotExecutor;
    }

    public ExecutorService getCliRpcExecutor() {
        return cliRpcExecutor;
    }

    public void setCliRpcExecutor(ExecutorService cliRpcExecutor) {
        this.cliRpcExecutor = cliRpcExecutor;
    }

    public ExecutorService getRaftRpcExecutor() {
        return raftRpcExecutor;
    }

    public void setRaftRpcExecutor(ExecutorService raftRpcExecutor) {
        this.raftRpcExecutor = raftRpcExecutor;
    }

    public ExecutorService getKvRpcExecutor() {
        return kvRpcExecutor;
    }

    public void setKvRpcExecutor(ExecutorService kvRpcExecutor) {
        this.kvRpcExecutor = kvRpcExecutor;
    }

    public ScheduledExecutorService getMetricsScheduler() {
        return metricsScheduler;
    }

    public void setMetricsScheduler(ScheduledExecutorService metricsScheduler) {
        this.metricsScheduler = metricsScheduler;
    }

    public ScheduledReporter getKvMetricsReporter() {
        return kvMetricsReporter;
    }

    public void setKvMetricsReporter(ScheduledReporter kvMetricsReporter) {
        this.kvMetricsReporter = kvMetricsReporter;
    }

    public ScheduledReporter getThreadPoolMetricsReporter() {
        return threadPoolMetricsReporter;
    }

    public void setThreadPoolMetricsReporter(ScheduledReporter threadPoolMetricsReporter) {
        this.threadPoolMetricsReporter = threadPoolMetricsReporter;
    }

    public boolean removeAndStopRegionEngine(final long regionId) {
        final RegionEngine engine = this.regionEngineTable.get(regionId);
        if (engine != null) {
            engine.shutdown();
            return true;
        }
        return false;
    }

    public StateListenerContainer<Long> getStateListenerContainer() {
        return stateListenerContainer;
    }

    public List<Long> getLeaderRegionIds() {
        final List<Long> regionIds = Lists.newArrayListWithCapacity(this.regionEngineTable.size());
        for (final RegionEngine regionEngine : this.regionEngineTable.values()) {
            if (regionEngine.isLeader()) {
                regionIds.add(regionEngine.getRegion().getId());
            }
        }
        return regionIds;
    }

    public int getRegionCount() {
        return this.regionEngineTable.size();
    }

    public int getLeaderRegionCount() {
        int count = 0;
        for (final RegionEngine regionEngine : this.regionEngineTable.values()) {
            if (regionEngine.isLeader()) {
                count++;
            }
        }
        return count;
    }

    public boolean isBusy() {
        // Need more info
        return splitting.get();
    }

    public void applySplit(final Long regionId, final Long newRegionId, final KVStoreClosure closure) {
        Requires.requireNonNull(regionId, "regionId");
        Requires.requireNonNull(newRegionId, "newRegionId");
        if (this.regionEngineTable.containsKey(newRegionId)) {
            closure.setError(Errors.CONFLICT_REGION_ID);
            closure.run(new Status(-1, "Conflict region id %d", newRegionId));
            return;
        }
        if (!this.splitting.compareAndSet(false, true)) {
            closure.setError(Errors.SERVER_BUSY);
            closure.run(new Status(-1, "Server is busy now"));
            return;
        }
        final RegionEngine parentEngine = getRegionEngine(regionId);
        if (parentEngine == null) {
            closure.setError(Errors.NO_REGION_FOUND);
            closure.run(new Status(-1, "RegionEngine[%s] not found", regionId));
            this.splitting.set(false);
            return;
        }
        if (!parentEngine.isLeader()) {
            closure.setError(Errors.NOT_LEADER);
            closure.run(new Status(-1, "RegionEngine[%s] not leader", regionId));
            this.splitting.set(false);
            return;
        }
        final Region parentRegion = parentEngine.getRegion();
        final byte[] startKey = BytesUtil.nullToEmpty(parentRegion.getStartKey());
        final byte[] endKey = parentRegion.getEndKey();
        final long approximateKeys = this.rawKVStore.getApproximateKeysInRange(startKey, endKey);
        final long leastKeysOnSplit = this.storeOpts.getLeastKeysOnSplit();
        if (approximateKeys < leastKeysOnSplit) {
            closure.setError(Errors.TOO_SMALL_TO_SPLIT);
            closure.run(new Status(-1, "RegionEngine[%s]'s keys less than %d", regionId, leastKeysOnSplit));
            this.splitting.set(false);
            return;
        }
        final byte[] splitKey = this.rawKVStore.jumpOver(startKey, approximateKeys >> 1);
        if (splitKey == null) {
            closure.setError(Errors.STORAGE_ERROR);
            closure.run(new Status(-1, "Fail to scan split key"));
            this.splitting.set(false);
            return;
        }
        final KVOperation op = KVOperation.createRangeSplit(splitKey, regionId, newRegionId);
        final Task task = new Task();
        task.setData(ByteBuffer.wrap(Serializers.getDefault().writeObject(op)));
        task.setDone(new KVClosureAdapter(closure, op));
        parentEngine.getNode().apply(task);
    }

    public void doSplit(final Long regionId, final Long newRegionId, final byte[] splitKey) {
        try {
            Requires.requireNonNull(regionId, "regionId");
            Requires.requireNonNull(newRegionId, "newRegionId");
            final RegionEngine parent = getRegionEngine(regionId);
            final Region region = parent.getRegion().copy();
            final RegionEngineOptions rOpts = parent.copyRegionOpts();
            region.setId(newRegionId);
            region.setStartKey(splitKey);
            region.setRegionEpoch(new RegionEpoch(-1, -1));

            rOpts.setRegionId(newRegionId);
            rOpts.setStartKeyBytes(region.getStartKey());
            rOpts.setEndKeyBytes(region.getEndKey());
            rOpts.setRaftGroupId(JRaftHelper.getJRaftGroupId(this.pdClient.getClusterName(), newRegionId));
            rOpts.setRaftDataPath(null);

            String baseRaftDataPath = this.storeOpts.getRaftDataPath();
            if (Strings.isBlank(baseRaftDataPath)) {
                baseRaftDataPath = "";
            }
            rOpts.setRaftDataPath(baseRaftDataPath + "raft_data_region_" + region.getId() + "_"
                                  + getSelfEndpoint().getPort());
            final RegionEngine engine = new RegionEngine(region, this);
            if (!engine.init(rOpts)) {
                LOG.error("Fail to init [RegionEngine: {}].", region);
                throw Errors.REGION_ENGINE_FAIL.exception();
            }

            // update parent conf
            final Region pRegion = parent.getRegion();
            final RegionEpoch pEpoch = pRegion.getRegionEpoch();
            final long version = pEpoch.getVersion();
            pEpoch.setVersion(version + 1); // version + 1
            pRegion.setEndKey(splitKey); // update endKey

            // the following two lines of code can make a relation of 'happens-before' for
            // read 'pRegion', because that a write to a ConcurrentMap happens-before every
            // subsequent read of that ConcurrentMap.
            this.regionEngineTable.put(region.getId(), engine);
            registerRegionKVService(new DefaultRegionKVService(engine));

            // update local regionRouteTable
            this.pdClient.getRegionRouteTable().splitRegion(pRegion.getId(), region);
        } finally {
            this.splitting.set(false);
        }
    }

    private void startMetricReporters(final long metricsReportPeriod) {
        if (metricsReportPeriod <= 0) {
            return;
        }
        if (this.kvMetricsReporter == null) {
            if (this.metricsScheduler == null) {
                // will sharing with all regionEngines
                this.metricsScheduler = StoreEngineHelper.createMetricsScheduler();
            }
            // start kv store metrics reporter
            this.kvMetricsReporter = Slf4jReporter.forRegistry(KVMetrics.metricRegistry()) //
                .prefixedWith("store_" + this.storeId) //
                .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO) //
                .outputTo(LOG) //
                .scheduleOn(this.metricsScheduler) //
                .shutdownExecutorOnStop(false) //
                .build();
            this.kvMetricsReporter.start(metricsReportPeriod, TimeUnit.SECONDS);
        }
        if (this.threadPoolMetricsReporter == null) {
            if (this.metricsScheduler == null) {
                // will sharing with all regionEngines
                this.metricsScheduler = StoreEngineHelper.createMetricsScheduler();
            }
            // start threadPool metrics reporter
            this.threadPoolMetricsReporter = Slf4jReporter.forRegistry(ThreadPoolMetricRegistry.metricRegistry()) //
                .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO) //
                .outputTo(LOG) //
                .scheduleOn(this.metricsScheduler) //
                .shutdownExecutorOnStop(false) //
                .build();
            this.threadPoolMetricsReporter.start(metricsReportPeriod, TimeUnit.SECONDS);
        }
    }

    private boolean initRawKVStore(final StoreEngineOptions opts) {
        final StorageType storageType = opts.getStorageType();
        switch (storageType) {
            case RocksDB:
                return initRocksDB(opts);
            case Memory:
                return initMemoryDB(opts);
            default:
                throw new UnsupportedOperationException("unsupported storage type: " + storageType);
        }
    }

    private boolean initRocksDB(final StoreEngineOptions opts) {
        RocksDBOptions rocksOpts = opts.getRocksDBOptions();
        if (rocksOpts == null) {
            rocksOpts = new RocksDBOptions();
            opts.setRocksDBOptions(rocksOpts);
        }
        String dbPath = rocksOpts.getDbPath();
        if (Strings.isNotBlank(dbPath)) {
            try {
                FileUtils.forceMkdir(new File(dbPath));
            } catch (final Throwable t) {
                LOG.error("Fail to make dir for dbPath {}.", dbPath);
                return false;
            }
        } else {
            dbPath = "";
        }
        final String childPath = "db_" + this.storeId + "_" + opts.getServerAddress().getPort();
        rocksOpts.setDbPath(Paths.get(dbPath, childPath).toString());
        this.dbPath = new File(rocksOpts.getDbPath());
        final RocksRawKVStore rocksRawKVStore = new RocksRawKVStore();
        if (!rocksRawKVStore.init(rocksOpts)) {
            LOG.error("Fail to init [RocksRawKVStore].");
            return false;
        }
        this.rawKVStore = rocksRawKVStore;
        return true;
    }

    private boolean initMemoryDB(final StoreEngineOptions opts) {
        MemoryDBOptions memoryOpts = opts.getMemoryDBOptions();
        if (memoryOpts == null) {
            memoryOpts = new MemoryDBOptions();
            opts.setMemoryDBOptions(memoryOpts);
        }
        final MemoryRawKVStore memoryRawKVStore = new MemoryRawKVStore();
        if (!memoryRawKVStore.init(memoryOpts)) {
            LOG.error("Fail to init [MemoryRawKVStore].");
            return false;
        }
        this.rawKVStore = memoryRawKVStore;
        return true;
    }

    private boolean initAllRegionEngine(final StoreEngineOptions opts, final Store store) {
        Requires.requireNonNull(opts, "opts");
        Requires.requireNonNull(store, "store");
        String baseRaftDataPath = opts.getRaftDataPath();
        if (Strings.isNotBlank(baseRaftDataPath)) {
            try {
                FileUtils.forceMkdir(new File(baseRaftDataPath));
            } catch (final Throwable t) {
                LOG.error("Fail to make dir for raftDataPath: {}.", baseRaftDataPath);
                return false;
            }
        } else {
            baseRaftDataPath = "";
        }
        final Endpoint serverAddress = opts.getServerAddress();
        final List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        final List<Region> regionList = store.getRegions();
        Requires.requireTrue(rOptsList.size() == regionList.size());
        for (int i = 0; i < rOptsList.size(); i++) {
            final RegionEngineOptions rOpts = rOptsList.get(i);
            if (!inConfiguration(rOpts.getServerAddress().toString(), rOpts.getInitialServerList())) {
                continue;
            }
            final Region region = regionList.get(i);
            if (Strings.isBlank(rOpts.getRaftDataPath())) {
                final String childPath = "raft_data_region_" + region.getId() + "_" + serverAddress.getPort();
                rOpts.setRaftDataPath(Paths.get(baseRaftDataPath, childPath).toString());
            }
            Requires.requireNonNull(region.getRegionEpoch(), "regionEpoch");
            final RegionEngine engine = new RegionEngine(region, this);
            if (engine.init(rOpts)) {
                final RegionKVService regionKVService = new DefaultRegionKVService(engine);
                registerRegionKVService(regionKVService);
                this.regionEngineTable.put(region.getId(), engine);
            } else {
                LOG.error("Fail to init [RegionEngine: {}].", region);
                return false;
            }
        }
        return true;
    }

    private boolean inConfiguration(final String curr, final String all) {
        final PeerId currPeer = new PeerId();
        if (!currPeer.parse(curr)) {
            return false;
        }
        final Configuration allConf = new Configuration();
        if (!allConf.parse(all)) {
            return false;
        }
        return allConf.contains(currPeer) || allConf.getLearners().contains(currPeer);
    }

    private void registerRegionKVService(final RegionKVService regionKVService) {
        final RegionKVService preService = this.regionKVServiceTable.putIfAbsent(regionKVService.getRegionId(),
            regionKVService);
        if (preService != null) {
            throw new RheaRuntimeException("RegionKVService[region=" + regionKVService.getRegionId()
                                           + "] has already been registered, can not register again!");
        }
    }

    @Override
    public String toString() {
        return "StoreEngine{storeId=" + storeId + ", startTime=" + startTime + ", dbPath=" + dbPath + ", storeOpts="
               + storeOpts + ", started=" + started + ", regions=" + getAllRegionEngines() + '}';
    }

    @Override
    public void describe(final Printer out) {
        out.println("StoreEngine:"); //
        out.print("  AllLeaderRegions:") //
            .println(getLeaderRegionIds()); //
        for (final RegionEngine r : getAllRegionEngines()) {
            r.describe(out);
        }
    }
}
