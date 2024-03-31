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
package com.alipay.sofa.jraft.rhea.storage;

/**
 * Seperation of concerns for batch put operations from {@link BatchRawKVStore}.
 *
 * @author jiachun.fjc + JAYDIPSINH27
 */
public class BatchRawPutOperations<T> {
    private final BatchRawKVStore<T> store;

    public BatchRawPutOperations(BatchRawKVStore<T> store) {
        this.store = store;
    }

    public void batchPut(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            store.put(op.getKey(), op.getValue(), kvState.getDone());
        }
    }

    public void batchPutIfAbsent(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            store.putIfAbsent(op.getKey(), op.getValue(), kvState.getDone());
        }
    }

    public void batchPutList(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            store.put(kvState.getOp().getEntries(), kvState.getDone());
        }
    }
}
