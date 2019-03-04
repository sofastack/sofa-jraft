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

/**
 *
 * @author jiachun.fjc
 */
public class BatchingOptions {

    // If batching is allowed, the client will submit the requests in batch mode,
    // which will improve the throughput without any negative impact on the delay.
    private boolean allowBatching = true;
    // Maximum number of requests that can be applied in a batch.
    private int     batchSize     = 100;
    // Internal disruptor buffers size for get/put request etc.
    private int     bufSize       = 8192;
    // Maximum bytes size to cached for put-request (keys.size + value.size).
    private int     maxWriteBytes = 32768;
    // Maximum bytes size to cached for get-request (keys.size).
    private int     maxReadBytes  = 1024;

    public boolean isAllowBatching() {
        return allowBatching;
    }

    public void setAllowBatching(boolean allowBatching) {
        this.allowBatching = allowBatching;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBufSize() {
        return bufSize;
    }

    public void setBufSize(int bufSize) {
        this.bufSize = bufSize;
    }

    public int getMaxWriteBytes() {
        return maxWriteBytes;
    }

    public void setMaxWriteBytes(int maxWriteBytes) {
        this.maxWriteBytes = maxWriteBytes;
    }

    public int getMaxReadBytes() {
        return maxReadBytes;
    }

    public void setMaxReadBytes(int maxReadBytes) {
        this.maxReadBytes = maxReadBytes;
    }

    @Override
    public String toString() {
        return "BatchingOptions{" + "allowBatching=" + allowBatching + ", batchSize=" + batchSize + ", bufSize="
               + bufSize + ", maxWriteBytes=" + maxWriteBytes + ", maxReadBytes=" + maxReadBytes + '}';
    }
}
