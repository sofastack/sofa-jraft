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
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.util.BloomFilter;

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

    private void doCalculate(final KVOperation operation, final BloomFilter<byte[]> bloomFilter) {

    }
}
