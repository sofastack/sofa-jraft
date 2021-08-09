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

import com.alipay.sofa.jraft.rhea.util.BloomFilter;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hzh (642256541@qq.com)
 */
public class RecyclableKvTaskTest {

    @Test
    public void testRecycle() {
        String key1 = "key1";
        String key2 = "key2";
        final RecyclableKvTask task = RecyclableKvTask.newInstance();
        final BloomFilter<byte[]> bloomFilter = task.getFilter();
        {
            bloomFilter.add(BytesUtil.writeUtf8(key1));
            Assert.assertTrue(bloomFilter.contains(BytesUtil.writeUtf8(key1)));
            Assert.assertFalse(bloomFilter.contains(BytesUtil.writeUtf8(key2)));
        }
        {
            task.recycle();
            Assert.assertFalse(bloomFilter.contains(BytesUtil.writeUtf8(key1)));
        }
    }
}