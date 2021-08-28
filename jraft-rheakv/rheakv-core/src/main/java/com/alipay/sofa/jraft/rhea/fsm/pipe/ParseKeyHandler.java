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
package com.alipay.sofa.jraft.rhea.fsm.pipe;

import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.lmax.disruptor.WorkHandler;
import com.alipay.sofa.jraft.rhea.fsm.ParallelPipeline.KvEvent;
import java.util.ArrayList;
import java.util.List;

/**
 * Parse key from kvStateList
 * @author hzh (642256541@qq.com)
 */
public class ParseKeyHandler implements WorkHandler<KvEvent> {

    public ParseKeyHandler() {
    }

    @Override
    public void onEvent(final KvEvent event) {
        final RecyclableKvTask task = event.getTask();
        final BloomFilter bloomFilter = task.getFilter();
        final List<KVState> kvStateList = task.getKvStateList();
        final List<byte[]> waitToAddKeyList = new ArrayList<>(kvStateList.size());
        // Parse all keys
        for (final KVState kvState : kvStateList) {
            parseKey(kvState, waitToAddKeyList, task);
        }
        // Map all keys to bloomFilter, calculate boundary key(min, max)
        bloomFilter.addAll(waitToAddKeyList);
        task.updateBoundaryKey(waitToAddKeyList);
    }

    /**
     *
     * Parse all key by switch specific operations
     */
    public void parseKey(final KVState kvState, final List<byte[]> waitToAddKeyList, final RecyclableKvTask task) {
        final KVOperation kvOp = kvState.getOp();
        final byte op = kvOp.getOp();
        switch (op) {
            case KVOperation.PUT_LIST: {
                // KvEntryList
                final List<KVEntry> entries = kvOp.getEntries();
                for (final KVEntry entry : entries) {
                    if (entry.getKey() != null) {
                        waitToAddKeyList.add(entry.getKey());
                    }
                }
                return;
            }
            case KVOperation.DELETE_LIST:
            case KVOperation.MULTI_GET: {
                // KeyList
                final List<byte[]> keyList = kvOp.getKeyList();
                waitToAddKeyList.addAll(keyList);
                return;
            }
            case KVOperation.COMPARE_PUT_ALL: {
                // CASEntryList
                final List<CASEntry> casEntries = kvOp.getCASEntries();
                for (final CASEntry casEntry : casEntries) {
                    if (casEntry.getKey() != null) {
                        waitToAddKeyList.add(casEntry.getKey());
                    }
                }
                return;
            }
            case KVOperation.KEY_LOCK:
            case KVOperation.KEY_LOCK_RELEASE: {
                // Key, fencingKey
                final byte[] key = kvOp.getKey();
                final byte[] fencingKey = kvOp.getFencingKey();
                if (key != null) {
                    waitToAddKeyList.add(key);
                }
                if (fencingKey != null) {
                    waitToAddKeyList.add(fencingKey);
                }
                return;
            }
            case KVOperation.SCAN:
            case KVOperation.REVERSE_SCAN:
            case KVOperation.DELETE_RANGE: {
                // Range related operation, add key pair
                final String startKey = BytesUtil.readUtf8(kvOp.getStartKey());
                final String endKey = BytesUtil.readUtf8(kvOp.getEndKey());
                task.addRangeKeyPair(Pair.of(startKey, endKey));
                return;
            }
            case KVOperation.NODE_EXECUTE: {
                // Ignored
                return;
            }
            case KVOperation.MERGE:
            case KVOperation.RANGE_SPLIT: {
                // Region related operation
                task.addRegionOperation(kvState);
                return;
            }
            default: {
                // Single key
                final byte[] key = kvOp.getKey();
                if (key != null) {
                    waitToAddKeyList.add(key);
                }
            }
        }
    }
}
