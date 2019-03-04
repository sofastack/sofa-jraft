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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.metadata.StoreStats;
import com.alipay.sofa.jraft.rhea.metadata.TimeInterval;
import com.alipay.sofa.jraft.rhea.metrics.KVMetricNames;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.rocks.support.RocksStatistics;
import com.alipay.sofa.jraft.rhea.storage.BaseRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.codahale.metrics.Counter;

import static org.rocksdb.TickerType.BYTES_READ;
import static org.rocksdb.TickerType.BYTES_WRITTEN;
import static org.rocksdb.TickerType.NUMBER_KEYS_READ;
import static org.rocksdb.TickerType.NUMBER_KEYS_WRITTEN;
import static org.rocksdb.TickerType.NUMBER_MULTIGET_BYTES_READ;
import static org.rocksdb.TickerType.NUMBER_MULTIGET_KEYS_READ;

/**
 *
 * @author jiachun.fjc
 */
public class StatsCollector {

    private static final Logger     LOG = LoggerFactory.getLogger(StatsCollector.class);

    private final StoreEngine       storeEngine;
    private final BaseRawKVStore<?> rawKVStore;
    private final RocksRawKVStore   rocksRawKVStore;

    public StatsCollector(StoreEngine storeEngine) {
        this.storeEngine = storeEngine;
        this.rawKVStore = storeEngine.getRawKVStore();
        RocksRawKVStore store = null;
        if (storeEngine.getStoreOpts().getStorageType() == StorageType.RocksDB) {
            store = (RocksRawKVStore) rawKVStore;
        }
        this.rocksRawKVStore = store;
    }

    public StoreStats collectStoreStats(final TimeInterval timeInterval) {
        final StoreStats stats = new StoreStats();
        stats.setStoreId(this.storeEngine.getStoreId());
        // Capacity for the store
        stats.setCapacity(this.storeEngine.getTotalSpace());
        // Available size for the store
        stats.setAvailable(this.storeEngine.getUsableSpace());
        // Total region count in this store
        stats.setRegionCount(this.storeEngine.getRegionCount());
        // Leader region count in this store
        stats.setLeaderRegionCount(this.storeEngine.getLeaderRegionCount());
        // Current sending snapshot count
        // TODO
        // Current receiving snapshot count
        // TODO
        // How many region is applying snapshot
        // TODO
        // When the store is started (unix timestamp in milliseconds)
        stats.setStartTime(this.storeEngine.getStartTime());
        // If the store is busy
        stats.setBusy(this.storeEngine.isBusy());
        // Actually used space by db
        stats.setUsedSize(this.storeEngine.getStoreUsedSpace());
        // Bytes written for the store during this period
        stats.setBytesWritten(getStoreBytesWritten(true));
        // Bytes read for the store during this period
        stats.setBytesRead(getStoreBytesRead(true));
        // Keys written for the store during this period
        stats.setKeysWritten(getStoreKeysWritten(true));
        // Keys read for the store during this period
        stats.setKeysRead(getStoreKeysRead(true));
        // Actually reported time interval
        stats.setInterval(timeInterval);
        LOG.info("Collect [StoreStats]: {}.", stats);
        return stats;
    }

    public RegionStats collectRegionStats(final Region region, final TimeInterval timeInterval) {
        final RegionStats stats = new RegionStats();
        stats.setRegionId(region.getId());
        // Leader Peer sending the heartbeat
        stats.setLeader(new Peer(region.getId(), this.storeEngine.getStoreId(), this.storeEngine.getSelfEndpoint()));
        // Leader considers that these peers are down
        // TODO
        // Pending peers are the peers that the leader can't consider as working followers
        // TODO
        // Bytes written for the region during this period
        stats.setBytesWritten(getRegionBytesWritten(region, true));
        // Bytes read for the region during this period
        stats.setBytesRead(getRegionBytesRead(region, true));
        // Keys written for the region during this period
        stats.setKeysWritten(getRegionKeysWritten(region, true));
        // Keys read for the region during this period
        stats.setKeysRead(getRegionKeysRead(region, true));
        // Approximate region size
        // TODO very important
        // Approximate number of keys
        stats.setApproximateKeys(this.rawKVStore.getApproximateKeysInRange(region.getStartKey(), region.getEndKey()));
        // Actually reported time interval
        stats.setInterval(timeInterval);
        LOG.info("Collect [RegionStats]: {}.", stats);
        return stats;
    }

    public long getStoreBytesWritten(final boolean reset) {
        if (this.rocksRawKVStore == null) {
            return 0; // TODO memory db statistics
        }
        if (reset) {
            return RocksStatistics.getAndResetTickerCount(this.rocksRawKVStore, BYTES_WRITTEN);
        }
        return RocksStatistics.getTickerCount(this.rocksRawKVStore, BYTES_WRITTEN);
    }

    public long getStoreBytesRead(final boolean reset) {
        if (this.rocksRawKVStore == null) {
            return 0; // TODO memory db statistics
        }
        if (reset) {
            return RocksStatistics.getAndResetTickerCount(this.rocksRawKVStore, BYTES_READ)
                   + RocksStatistics.getAndResetTickerCount(this.rocksRawKVStore, NUMBER_MULTIGET_BYTES_READ);
        }
        return RocksStatistics.getTickerCount(this.rocksRawKVStore, BYTES_READ)
               + RocksStatistics.getTickerCount(this.rocksRawKVStore, NUMBER_MULTIGET_BYTES_READ);
    }

    public long getStoreKeysWritten(final boolean reset) {
        if (this.rocksRawKVStore == null) {
            return 0; // TODO memory db statistics
        }
        if (reset) {
            return RocksStatistics.getAndResetTickerCount(this.rocksRawKVStore, NUMBER_KEYS_WRITTEN);
        }
        return RocksStatistics.getTickerCount(this.rocksRawKVStore, NUMBER_KEYS_WRITTEN);
    }

    public long getStoreKeysRead(final boolean reset) {
        if (this.rocksRawKVStore == null) {
            return 0; // TODO memory db statistics
        }
        if (reset) {
            return RocksStatistics.getAndResetTickerCount(this.rocksRawKVStore, NUMBER_KEYS_READ)
                   + RocksStatistics.getAndResetTickerCount(this.rocksRawKVStore, NUMBER_MULTIGET_KEYS_READ);
        }
        return RocksStatistics.getTickerCount(this.rocksRawKVStore, NUMBER_KEYS_READ)
               + RocksStatistics.getTickerCount(this.rocksRawKVStore, NUMBER_MULTIGET_KEYS_READ);
    }

    public long getRegionBytesWritten(final Region region, final boolean reset) {
        final Counter counter = KVMetrics.counter(KVMetricNames.REGION_BYTES_WRITTEN, String.valueOf(region.getId()));
        final long value = counter.getCount();
        if (reset) {
            counter.dec(value);
        }
        return value;
    }

    public long getRegionBytesRead(final Region region, final boolean reset) {
        final Counter counter = KVMetrics.counter(KVMetricNames.REGION_BYTES_READ, String.valueOf(region.getId()));
        final long value = counter.getCount();
        if (reset) {
            counter.dec(value);
        }
        return value;
    }

    public long getRegionKeysWritten(final Region region, final boolean reset) {
        final Counter counter = KVMetrics.counter(KVMetricNames.REGION_KEYS_WRITTEN, String.valueOf(region.getId()));
        final long value = counter.getCount();
        if (reset) {
            counter.dec(value);
        }
        return value;
    }

    public long getRegionKeysRead(final Region region, final boolean reset) {
        final Counter counter = KVMetrics.counter(KVMetricNames.REGION_KEYS_READ, String.valueOf(region.getId()));
        final long value = counter.getCount();
        if (reset) {
            counter.dec(value);
        }
        return value;
    }
}
