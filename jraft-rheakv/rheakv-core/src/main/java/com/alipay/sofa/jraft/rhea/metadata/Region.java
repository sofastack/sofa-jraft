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
package com.alipay.sofa.jraft.rhea.metadata;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Copiable;

/**
 * Region is the most basic kv data unit.  Each region has a left-closed
 * right-open interval range.  The region is responsible for the crud
 * request in the range.  Each region is a raft group, which is distributed
 * on different store nodes. Each region copy is called a region peer.
 * As the data volume of the region reaches the threshold, it will trigger
 * split. In fact, it is only the metadata change. This action is very fast.
 * The two regions just after the split are still on the original store node.
 * The PD periodically checks the number of regions in each store node, and
 * the load situation. The raft snapshot is used to migrate the region between
 * the store nodes to ensure the load balance of the cluster.
 *
 * @author jiachun.fjc
 */
public class Region implements Copiable<Region>, Serializable {

    private static final long serialVersionUID        = -2610978803578899118L;

    // To distinguish the id automatically assigned by the PD,
    // the manually configured id ranges from [-1, 1000000L).
    public static final long  MIN_ID_WITH_MANUAL_CONF = -1L;
    public static final long  MAX_ID_WITH_MANUAL_CONF = 1000000L;

    private long              id;                                             // region id
    // Region key range [startKey, endKey)
    private byte[]            startKey;                                       // inclusive
    private byte[]            endKey;                                         // exclusive
    private RegionEpoch       regionEpoch;                                    // region term
    private List<Peer>        peers;                                          // all peers in the region

    public Region() {
    }

    public Region(long id, byte[] startKey, byte[] endKey, RegionEpoch regionEpoch, List<Peer> peers) {
        this.id = id;
        this.startKey = startKey;
        this.endKey = endKey;
        this.regionEpoch = regionEpoch;
        this.peers = peers;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

    public RegionEpoch getRegionEpoch() {
        return regionEpoch;
    }

    public void setRegionEpoch(RegionEpoch regionEpoch) {
        this.regionEpoch = regionEpoch;
    }

    public List<Peer> getPeers() {
        return peers;
    }

    public void setPeers(List<Peer> peers) {
        this.peers = peers;
    }

    @Override
    public Region copy() {
        RegionEpoch regionEpoch = null;
        if (this.regionEpoch != null) {
            regionEpoch = this.regionEpoch.copy();
        }
        List<Peer> peers = null;
        if (this.peers != null) {
            peers = Lists.newArrayListWithCapacity(this.peers.size());
            for (Peer peer : this.peers) {
                peers.add(peer.copy());
            }
        }
        return new Region(this.id, this.startKey, this.endKey, regionEpoch, peers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Region region = (Region) o;
        return id == region.id && Objects.equals(regionEpoch, region.regionEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, regionEpoch);
    }

    @Override
    public String toString() {
        return "Region{" + "id=" + id + ", startKey=" + BytesUtil.toHex(startKey) + ", endKey="
               + BytesUtil.toHex(endKey) + ", regionEpoch=" + regionEpoch + ", peers=" + peers + '}';
    }
}
