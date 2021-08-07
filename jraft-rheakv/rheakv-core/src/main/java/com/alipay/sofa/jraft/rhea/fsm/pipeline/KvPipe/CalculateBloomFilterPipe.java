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
 * Calculate the bitmap of KVOperation batch
 * @author hzh (642256541@qq.com)
 */
public class CalculateBloomFilterPipe extends AbstractPipe<List<KVState>, BatchWrapper> {

    @Override
    public BatchWrapper doProcess(final List<KVState> kvStateList) {
        final BloomFilter<byte[]> bloomFilter = RecyclableBloomFilter.newInstance().getBloomFilter();
        for (final KVState kvState : kvStateList) {
            doCalculate(kvState.getOp(), bloomFilter);
        }
        return new BatchWrapper(kvStateList, bloomFilter);
    }

    private void doCalculate(final KVOperation kvOp, final BloomFilter<byte[]> bloomFilter) {
        final byte op = kvOp.getOp();
        final List<byte[]> waitToAddKeyList = new ArrayList<>();
        switch (op) {
            case KVOperation.PUT_LIST: {
                // kvEntryList
                final List<KVEntry> entries = kvOp.getEntries();
                for (final KVEntry entry : entries) {
                    if (entry.getKey() != null) {
                        waitToAddKeyList.add(entry.getKey());
                    }
                }
            }
            case KVOperation.DELETE_LIST:
            case KVOperation.MULTI_GET: {
                // KeyBytesList
                final List<byte[]> keyList = kvOp.getKeyList();
                waitToAddKeyList.addAll(keyList);
            }
            case KVOperation.COMPARE_PUT_ALL: {
                // CASEntryList
                final List<CASEntry> casEntries = kvOp.getCASEntries();
                for (final CASEntry casEntry : casEntries) {
                    if (casEntry.getKey() != null) {
                        waitToAddKeyList.add(casEntry.getKey());
                    }
                }
            }
            case KVOperation.NODE_EXECUTE:
            case KVOperation.RANGE_SPLIT:
            case KVOperation.MERGE:
            case KVOperation.DELETE_RANGE:
            case KVOperation.SCAN:
            case KVOperation.REVERSE_SCAN: {
                // Can't calculate
                return;
            }
            default: {
                // Single key
                final byte[] fencingKey = kvOp.getFencingKey();
                final byte[] key = kvOp.getKey();
                final byte[] seqKey = kvOp.getSeqKey();
                if (fencingKey != null) {
                    waitToAddKeyList.add(fencingKey);
                }
                if (key != null) {
                    waitToAddKeyList.add(key);
                }
                if (seqKey != null) {
                    waitToAddKeyList.add(seqKey);
                }
            }
        }
        bloomFilter.addAll(waitToAddKeyList);
    }

}
