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
package com.alipay.sofa.jraft.entity;

import java.util.List;

import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;

/**
 * ReadIndex requests statuses.
 * @author dennis
 *
 */
public class ReadIndexStatus {

    private final ReadIndexRequest     request; // raw request
    private final List<ReadIndexState> states; // read index requests in batch.
    private final long                 index;  // committed log index.

    public ReadIndexStatus(List<ReadIndexState> states, ReadIndexRequest request, long index) {
        super();
        this.index = index;
        this.request = request;
        this.states = states;
    }

    public boolean isApplied(long appliedIndex) {
        return appliedIndex >= this.index;
    }

    public boolean isOverMaxReadIndexLag(long applyIndex, int maxReadIndexLag) {
        if (maxReadIndexLag < 0) {
            return false;
        }
        return this.index - applyIndex > maxReadIndexLag;
    }

    public long getIndex() {
        return index;
    }

    public ReadIndexRequest getRequest() {
        return request;
    }

    public List<ReadIndexState> getStates() {
        return states;
    }

}