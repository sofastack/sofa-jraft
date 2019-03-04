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

/**
 *
 * @author jiachun.fjc
 */
public class RegionStats implements Serializable {

    private static final long serialVersionUID = -4790131346095689335L;

    private long              regionId;
    // Leader Peer sending the heartbeat
    private Peer              leader;
    // Leader considers that these peers are down
    private List<PeerStats>   downPeers;
    // Pending peers are the peers that the leader can't consider as working followers
    private List<PeerStats>   pendingPeers;
    // Bytes written for the region during this period
    private long              bytesWritten;
    // Bytes read for the region during this period
    private long              bytesRead;
    // Keys written for the region during this period
    private long              keysWritten;
    // Keys read for the region during this period
    private long              keysRead;
    // Approximate region size
    private long              approximateSize;
    // Approximate number of keys
    private long              approximateKeys;
    // Actually reported time interval
    private TimeInterval      interval;

    public long getRegionId() {
        return regionId;
    }

    public void setRegionId(long regionId) {
        this.regionId = regionId;
    }

    public Peer getLeader() {
        return leader;
    }

    public void setLeader(Peer leader) {
        this.leader = leader;
    }

    public List<PeerStats> getDownPeers() {
        return downPeers;
    }

    public void setDownPeers(List<PeerStats> downPeers) {
        this.downPeers = downPeers;
    }

    public List<PeerStats> getPendingPeers() {
        return pendingPeers;
    }

    public void setPendingPeers(List<PeerStats> pendingPeers) {
        this.pendingPeers = pendingPeers;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public long getKeysWritten() {
        return keysWritten;
    }

    public void setKeysWritten(long keysWritten) {
        this.keysWritten = keysWritten;
    }

    public long getKeysRead() {
        return keysRead;
    }

    public void setKeysRead(long keysRead) {
        this.keysRead = keysRead;
    }

    public long getApproximateSize() {
        return approximateSize;
    }

    public void setApproximateSize(long approximateSize) {
        this.approximateSize = approximateSize;
    }

    public long getApproximateKeys() {
        return approximateKeys;
    }

    public void setApproximateKeys(long approximateKeys) {
        this.approximateKeys = approximateKeys;
    }

    public TimeInterval getInterval() {
        return interval;
    }

    public void setInterval(TimeInterval interval) {
        this.interval = interval;
    }

    @Override
    public String toString() {
        return "RegionStats{" + "regionId=" + regionId + ", leader=" + leader + ", downPeers=" + downPeers
               + ", pendingPeers=" + pendingPeers + ", bytesWritten=" + bytesWritten + ", bytesRead=" + bytesRead
               + ", keysWritten=" + keysWritten + ", keysRead=" + keysRead + ", approximateSize=" + approximateSize
               + ", approximateKeys=" + approximateKeys + ", interval=" + interval + '}';
    }
}
