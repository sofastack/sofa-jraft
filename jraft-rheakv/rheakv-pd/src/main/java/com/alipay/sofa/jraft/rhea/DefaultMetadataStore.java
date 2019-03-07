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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.metadata.Cluster;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.metadata.StoreStats;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.LongSequence;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 *
 * @author jiachun.fjc
 */
public class DefaultMetadataStore implements MetadataStore {

    private static final Logger                       LOG                  = LoggerFactory
                                                                               .getLogger(DefaultMetadataStore.class);

    private final ConcurrentMap<String, LongSequence> storeSequenceMap     = Maps.newConcurrentMap();
    private final ConcurrentMap<String, LongSequence> regionSequenceMap    = Maps.newConcurrentMap();
    private final ConcurrentMap<Long, Set<Long>>      clusterStoreIdsCache = Maps.newConcurrentMapLong();
    private final Serializer                          serializer           = Serializers.getDefault();
    private final RheaKVStore                         rheaKVStore;

    public DefaultMetadataStore(RheaKVStore rheaKVStore) {
        this.rheaKVStore = rheaKVStore;
    }

    @Override
    public Cluster getClusterInfo(final long clusterId) {
        final Set<Long> storeIds = getClusterIndex(clusterId);
        if (storeIds == null) {
            return null;
        }
        final List<byte[]> storeKeys = Lists.newArrayList();
        for (final Long storeId : storeIds) {
            final String storeInfoKey = MetadataKeyHelper.getStoreInfoKey(clusterId, storeId);
            storeKeys.add(BytesUtil.writeUtf8(storeInfoKey));
        }
        final Map<ByteArray, byte[]> storeInfoBytes = this.rheaKVStore.bMultiGet(storeKeys);
        final List<Store> stores = Lists.newArrayListWithCapacity(storeInfoBytes.size());
        for (final byte[] storeBytes : storeInfoBytes.values()) {
            final Store store = this.serializer.readObject(storeBytes, Store.class);
            stores.add(store);
        }
        return new Cluster(clusterId, stores);
    }

    @Override
    public Long getOrCreateStoreId(final long clusterId, final Endpoint endpoint) {
        final String storeIdKey = MetadataKeyHelper.getStoreIdKey(clusterId, endpoint);
        final byte[] bytesVal = this.rheaKVStore.bGet(storeIdKey);
        if (bytesVal == null) {
            final String storeSeqKey = MetadataKeyHelper.getStoreSeqKey(clusterId);
            LongSequence storeSequence = this.storeSequenceMap.get(storeSeqKey);
            if (storeSequence == null) {
                final LongSequence newStoreSequence = new LongSequence() {

                    @Override
                    public Sequence getNextSequence() {
                        return rheaKVStore.bGetSequence(storeSeqKey, 32);
                    }
                };
                storeSequence = this.storeSequenceMap.putIfAbsent(storeSeqKey, newStoreSequence);
                if (storeSequence == null) {
                    storeSequence = newStoreSequence;
                }
            }
            final long newStoreId = storeSequence.next();
            final byte[] newBytesVal = new byte[8];
            Bits.putLong(newBytesVal, 0, newStoreId);
            final byte[] oldBytesVal = this.rheaKVStore.bPutIfAbsent(storeIdKey, newBytesVal);
            if (oldBytesVal != null) {
                return Bits.getLong(oldBytesVal, 0);
            } else {
                return newStoreId;
            }
        }
        return Bits.getLong(bytesVal, 0);
    }

    @Override
    public Store getStoreInfo(final long clusterId, final long storeId) {
        final String storeInfoKey = MetadataKeyHelper.getStoreInfoKey(clusterId, storeId);
        final byte[] bytes = this.rheaKVStore.bGet(storeInfoKey);
        if (bytes == null) {
            Store empty = new Store();
            empty.setId(storeId);
            return empty;
        }
        return this.serializer.readObject(bytes, Store.class);
    }

    @Override
    public Store getStoreInfo(final long clusterId, final Endpoint endpoint) {
        final long storeId = getOrCreateStoreId(clusterId, endpoint);
        return getStoreInfo(clusterId, storeId);
    }

    @Override
    public CompletableFuture<Store> updateStoreInfo(final long clusterId, final Store store) {
        final long storeId = store.getId();
        final String storeInfoKey = MetadataKeyHelper.getStoreInfoKey(clusterId, storeId);
        final byte[] bytes = this.serializer.writeObject(store);
        final CompletableFuture<Store> future = new CompletableFuture<>();
        this.rheaKVStore.getAndPut(storeInfoKey, bytes).whenComplete((prevBytes, getPutThrowable) -> {
            if (getPutThrowable == null) {
                if (prevBytes != null) {
                    future.complete(serializer.readObject(prevBytes, Store.class));
                } else {
                    mergeClusterIndex(clusterId, storeId).whenComplete((ignored, mergeThrowable) -> {
                        if (mergeThrowable == null) {
                            future.complete(null);
                        } else {
                            future.completeExceptionally(mergeThrowable);
                        }
                    });
                }
            } else {
                future.completeExceptionally(getPutThrowable);
            }
        });
        return future;
    }

    @Override
    public Long createRegionId(final long clusterId) {
        final String regionSeqKey = MetadataKeyHelper.getRegionSeqKey(clusterId);
        LongSequence regionSequence = this.regionSequenceMap.get(regionSeqKey);
        if (regionSequence == null) {
            final LongSequence newRegionSequence = new LongSequence(Region.MAX_ID_WITH_MANUAL_CONF) {

                @Override
                public Sequence getNextSequence() {
                    return rheaKVStore.bGetSequence(regionSeqKey, 32);
                }
            };
            regionSequence = this.regionSequenceMap.putIfAbsent(regionSeqKey, newRegionSequence);
            if (regionSequence == null) {
                regionSequence = newRegionSequence;
            }
        }
        return regionSequence.next();
    }

    @Override
    public StoreStats getStoreStats(final long clusterId, final long storeId) {
        final String key = MetadataKeyHelper.getStoreStatsKey(clusterId, storeId);
        final byte[] bytes = this.rheaKVStore.bGet(key);
        if (bytes == null) {
            return null;
        }
        return this.serializer.readObject(bytes, StoreStats.class);
    }

    @Override
    public CompletableFuture<Boolean> updateStoreStats(final long clusterId, final StoreStats storeStats) {
        final String key = MetadataKeyHelper.getStoreStatsKey(clusterId, storeStats.getStoreId());
        final byte[] bytes = this.serializer.writeObject(storeStats);
        return this.rheaKVStore.put(key, bytes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<Region, RegionStats> getRegionStats(final long clusterId, final Region region) {
        final String key = MetadataKeyHelper.getRegionStatsKey(clusterId, region.getId());
        final byte[] bytes = this.rheaKVStore.bGet(key);
        if (bytes == null) {
            return null;
        }
        return this.serializer.readObject(bytes, Pair.class);
    }

    @Override
    public CompletableFuture<Boolean> updateRegionStats(final long clusterId, final Region region,
                                                        final RegionStats regionStats) {
        final String key = MetadataKeyHelper.getRegionStatsKey(clusterId, region.getId());
        final byte[] bytes = this.serializer.writeObject(Pair.of(region, regionStats));
        return this.rheaKVStore.put(key, bytes);
    }

    @Override
    public CompletableFuture<Boolean> batchUpdateRegionStats(final long clusterId,
                                                             final List<Pair<Region, RegionStats>> regionStatsList) {
        final List<KVEntry> entries = Lists.newArrayListWithCapacity(regionStatsList.size());
        for (final Pair<Region, RegionStats> p : regionStatsList) {
            final String key = MetadataKeyHelper.getRegionStatsKey(clusterId, p.getKey().getId());
            final byte[] bytes = this.serializer.writeObject(p);
            entries.add(new KVEntry(BytesUtil.writeUtf8(key), bytes));
        }
        return this.rheaKVStore.put(entries);
    }

    @Override
    public Set<Long> unsafeGetStoreIds(final long clusterId) {
        Set<Long> storeIds = this.clusterStoreIdsCache.get(clusterId);
        if (storeIds != null) {
            return storeIds;
        }
        storeIds = getClusterIndex(clusterId);
        this.clusterStoreIdsCache.put(clusterId, storeIds);
        return storeIds;
    }

    @Override
    public Map<Long, Endpoint> unsafeGetStoreIdsByEndpoints(final long clusterId, final List<Endpoint> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            return Collections.emptyMap();
        }
        final List<byte[]> storeIdKeyList = Lists.newArrayListWithCapacity(endpoints.size());
        final Map<ByteArray, Endpoint> keyToEndpointMap = Maps.newHashMapWithExpectedSize(endpoints.size());
        for (final Endpoint endpoint : endpoints) {
            final byte[] keyBytes = BytesUtil.writeUtf8(MetadataKeyHelper.getStoreIdKey(clusterId, endpoint));
            storeIdKeyList.add(keyBytes);
            keyToEndpointMap.put(ByteArray.wrap(keyBytes), endpoint);
        }
        final Map<ByteArray, byte[]> storeIdBytes = this.rheaKVStore.bMultiGet(storeIdKeyList);
        final Map<Long, Endpoint> ids = Maps.newHashMapWithExpectedSize(storeIdBytes.size());
        for (final Map.Entry<ByteArray, byte[]> entry : storeIdBytes.entrySet()) {
            final Long storeId = Bits.getLong(entry.getValue(), 0);
            final Endpoint endpoint = keyToEndpointMap.get(entry.getKey());
            ids.put(storeId, endpoint);
        }
        return ids;
    }

    @Override
    public void invalidCache() {
        this.clusterStoreIdsCache.clear();
    }

    private Set<Long> getClusterIndex(final long clusterId) {
        final String key = MetadataKeyHelper.getClusterInfoKey(clusterId);
        final byte[] indexBytes = this.rheaKVStore.bGet(key);
        if (indexBytes == null) {
            return null;
        }
        final String strVal = BytesUtil.readUtf8(indexBytes);
        final String[] array = Strings.split(strVal, ',');
        if (array == null) {
            return null;
        }
        final Set<Long> storeIds = new HashSet<>(array.length);
        for (final String str : array) {
            storeIds.add(Long.parseLong(str.trim()));
        }
        return storeIds;
    }

    private CompletableFuture<Boolean> mergeClusterIndex(final long clusterId, final long storeId) {
        final String key = MetadataKeyHelper.getClusterInfoKey(clusterId);
        final CompletableFuture<Boolean> future = this.rheaKVStore.merge(key, String.valueOf(storeId));
        future.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                LOG.error("Fail to merge cluster index, {}, {}.", key, StackTraceUtil.stackTrace(throwable));
            }
            clusterStoreIdsCache.clear();
        });
        return future;
    }
}
