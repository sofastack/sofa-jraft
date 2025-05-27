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
package com.alipay.sofa.jraft.core;

import java.util.Objects;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;

/**
 * RpcRequestHeader class supports both AppendEntriesRequest and SnapshotMeta headers.
 * It keeps the request metadata info without the data, designed to reduce memory consumption for RPC.
 * This class provides constructors for handling metadata from both types of requests.
 * @author dennis
 */
class RpcRequestHeader {
    final long         prevLogIndex;
    final long         prevLogTerm;
    final int          entriesCount;
    final int          dataBytes;
    final SnapshotMeta meta;

    public RpcRequestHeader(AppendEntriesRequest request) {
        super();
        this.prevLogIndex = request.getPrevLogIndex();
        this.prevLogTerm = request.getPrevLogTerm();
        this.entriesCount = request.getEntriesCount();
        this.dataBytes = request.getData() != null ? request.getData().size() : 0;
        this.meta = null;
    }

    @Override
    public String toString() {
        return "RpcRequestHeader [prevLogIndex=" + prevLogIndex + ", prevLogTerm=" + prevLogTerm + ", entriesCount="
               + entriesCount + ", dataBytes=" + dataBytes + ", meta=" + meta + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataBytes, entriesCount, meta, prevLogIndex, prevLogTerm);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RpcRequestHeader other = (RpcRequestHeader) obj;
        return dataBytes == other.dataBytes && entriesCount == other.entriesCount && Objects.equals(meta, other.meta)
               && prevLogIndex == other.prevLogIndex && prevLogTerm == other.prevLogTerm;
    }

    public RpcRequestHeader(SnapshotMeta meta) {
        super();
        this.prevLogIndex = 0;
        this.prevLogTerm = 0;
        this.entriesCount = 0;
        this.dataBytes = 0;
        this.meta = meta;
    }

    public SnapshotMeta getMeta() {
        return meta;
    }

    long getPrevLogIndex() {
        return prevLogIndex;
    }

    long getPrevLogTerm() {
        return prevLogTerm;
    }

    int getEntriesCount() {
        return entriesCount;
    }

    int getDataBytes() {
        return dataBytes;
    }
}
