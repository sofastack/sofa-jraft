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
import com.alipay.sofa.jraft.util.Recyclable;
import com.alipay.sofa.jraft.util.Recyclers;

/**
 * @author hzh (642256541@qq.com)
 */
public class RecyclableBloomFilter implements Recyclable {
    private final BloomFilter<byte[]> bloomFilter;

    public static RecyclableBloomFilter newInstance() {
        final RecyclableBloomFilter filter = recyclers.get();
        return filter;
    }

    @Override
    public boolean recycle() {
        this.bloomFilter.clear();
        return recyclers.recycle(this, handle);
    }

    private RecyclableBloomFilter(final Recyclers.Handle handle) {
        this.handle = handle;
        this.bloomFilter = new BloomFilter<byte[]>(0.1, 10000);
    }

    public BloomFilter<byte[]> getBloomFilter() {
        return bloomFilter;
    }

    private transient final Recyclers.Handle              handle;

    private static final Recyclers<RecyclableBloomFilter> recyclers = new Recyclers<RecyclableBloomFilter>(256) {
                                                                        @Override
                                                                        protected RecyclableBloomFilter newObject(final Handle handle) {
                                                                            return new RecyclableBloomFilter(handle);
                                                                        }
                                                                    };
}
