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

import com.alipay.sofa.jraft.rhea.fsm.pipeline.PipeBaseTest;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.util.BloomFilter;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.junit.Before;
import org.junit.Test;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author hzh (642256541@qq.com)
 */
public class KVPipelineTest extends PipeBaseTest {
    // Phase one
    private ReadKVOperationPipe      readPipe;

    // Phase two
    private CalculateBloomFilterPipe calculateBloomFilterPipe;

    private RecyclableBatchWrapper   batchWrapper;

    @Before
    public void init() {
        this.setup();
        this.readPipe = new ReadKVOperationPipe();
        this.calculateBloomFilterPipe = new CalculateBloomFilterPipe();
    }

    @Test
    public void testReadKvPipe() {
        int index = 0;
        this.batchWrapper = readPipe.doProcess(this.iter);
        final List<KVState> kvStateList = this.batchWrapper.getKvStateList();
        {
            assertEquals(kvStateList.size(), 10);
            for (final KVState kvState : kvStateList) {
                ++index;
                final String key = BytesUtil.readUtf8(kvState.getOp().getKey());
                assertEquals(key, "key-" + index);
            }
        }
    }

    @Test
    public void testCalculateBloomFilterPipe() {
        this.testReadKvPipe();
        this.calculateBloomFilterPipe.doProcess(this.batchWrapper);
        {
            final BloomFilter<byte[]> bloomFilter = batchWrapper.getFilter();
            for (final KVState kvState : batchWrapper.getKvStateList()) {
                final byte[] key = kvState.getOp().getKey();
                assertTrue(bloomFilter.contains(key));
            }
        }
    }

}
