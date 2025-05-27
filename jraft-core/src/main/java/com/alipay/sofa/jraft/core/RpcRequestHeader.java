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

import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;

/**
 * AppendEntriesRequest header, which only keeps the request metadata info without the data.
 * It's designed to reduce the memory consumption for RPC.
 * @author dennis
 *
 */
class RpcRequestHeader {
    final long         prevLogIndex;
    final long         prevLogTerm;
    final int          entriesCount;
    final int          dataBytes;
    final SnapshotMeta meta;

    public RpcRequestHeader(long prevLogIndex, long prevLogTerm, int entriesCount, int dataBytes) {
        super();
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entriesCount = entriesCount;
        this.dataBytes = dataBytes;
        this.meta = null;
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