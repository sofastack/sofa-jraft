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
package com.alipay.sofa.jraft.rhea.options;

import com.alipay.sofa.jraft.util.BytesUtil;

/**
 *
 * @author jiachun.fjc
 */
public class RegionRouteTableOptions {

    private Long   regionId;
    private String startKey;
    private byte[] startKeyBytes;
    private String endKey;
    private byte[] endKeyBytes;
    // Can extends from RheaKVStoreOptions
    private String initialServerList;

    public Long getRegionId() {
        return regionId;
    }

    public void setRegionId(Long regionId) {
        this.regionId = regionId;
    }

    public String getStartKey() {
        return startKey;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
        this.startKeyBytes = BytesUtil.writeUtf8(startKey);
    }

    public byte[] getStartKeyBytes() {
        return startKeyBytes;
    }

    public void setStartKeyBytes(byte[] startKeyBytes) {
        this.startKeyBytes = startKeyBytes;
    }

    public String getEndKey() {
        return endKey;
    }

    public void setEndKey(String endKey) {
        this.endKey = endKey;
        this.endKeyBytes = BytesUtil.writeUtf8(endKey);
    }

    public byte[] getEndKeyBytes() {
        return endKeyBytes;
    }

    public void setEndKeyBytes(byte[] endKeyBytes) {
        this.endKeyBytes = endKeyBytes;
    }

    public String getInitialServerList() {
        return initialServerList;
    }

    public void setInitialServerList(String initialServerList) {
        this.initialServerList = initialServerList;
    }

    @Override
    public String toString() {
        return "RegionRouteTableOptions{" + "regionId=" + regionId + ", startKey='" + startKey + '\''
               + ", startKeyBytes=" + BytesUtil.toHex(startKeyBytes) + ", endKey='" + endKey + '\'' + ", endKeyBytes="
               + BytesUtil.toHex(endKeyBytes) + ", initialServerList='" + initialServerList + '\'' + '}';
    }
}
