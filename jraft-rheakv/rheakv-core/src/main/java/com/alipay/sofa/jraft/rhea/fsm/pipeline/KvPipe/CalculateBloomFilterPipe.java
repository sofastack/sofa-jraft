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
package com.alipay.sofa.jraft.rhea.fsm.pipeline.KvPipe;

import com.alipay.sofa.jraft.rhea.fsm.pipeline.AbstractPipe;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.util.BloomFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * Calculate the bloomFilter of KVOperation batch
 * @author hzh (642256541@qq.com)
 */
public class CalculateBloomFilterPipe extends AbstractPipe<RecyclableKvTask, RecyclableKvTask> {

    @Override
    public RecyclableKvTask doProcess(final RecyclableKvTask task) {
        final BloomFilter<byte[]> bloomFilter = task.getFilter();
        final List<KVState> kvStateList = task.getKvStateList();
        final List<byte[]> waitToAddKeyList = new ArrayList(kvStateList.size());
        // Get all wait to be added keys
        for (final KVState kvState : kvStateList) {
            doGetOPKey(kvState.getOp(), waitToAddKeyList);
        }
        bloomFilter.addAll(waitToAddKeyList);
        return task;
    }

    /**
     * Get key of kvOp
     */
    public static void doGetOPKey(final KVOperation kvOp, final List<byte[]> waitToAddKeyList) {
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
                // KeyBytesList
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
            case KVOperation.GET_SEQUENCE:
            case KVOperation.RESET_SEQUENCE: {
                final byte[] seqKey = kvOp.getSeqKey();
                if (seqKey != null) {
                    waitToAddKeyList.add(seqKey);
                }
                return;
            }
            case KVOperation.NODE_EXECUTE:
            case KVOperation.RANGE_SPLIT:
            case KVOperation.MERGE:
            case KVOperation.DELETE_RANGE:
            case KVOperation.SCAN:
            case KVOperation.REVERSE_SCAN: {
                // Can't calculate key
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
