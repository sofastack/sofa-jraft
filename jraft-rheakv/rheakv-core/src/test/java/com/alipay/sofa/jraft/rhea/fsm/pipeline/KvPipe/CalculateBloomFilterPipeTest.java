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
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

/**
 * @author hzh (642256541@qq.com)
 */
public class CalculateBloomFilterPipeTest extends PipeBaseTest {
    // Phase one
    private ReadKVOperationPipe      readPipe;

    // Phase two
    private CalculateBloomFilterPipe calculateBloomFilterPipe;

    private RecyclableKvTask         task;

    @Before
    public void init() {
        this.setup();
        this.readPipe = new ReadKVOperationPipe();
        this.calculateBloomFilterPipe = new CalculateBloomFilterPipe();
        this.task = readPipe.doProcess(this.iter);
    }

    @Test
    public void testDoProcess() {
        this.calculateBloomFilterPipe.doProcess(this.task);
        {
            final BloomFilter<byte[]> bloomFilter = task.getFilter();
            for (final KVState kvState : task.getKvStateList()) {
                final byte[] key = kvState.getOp().getKey();
                assertTrue(bloomFilter.contains(key));
            }
        }
    }
}