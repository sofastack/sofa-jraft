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

import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.util.BloomFilter;
import com.alipay.sofa.jraft.util.Recyclable;
import com.alipay.sofa.jraft.util.Recyclers;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hzh (642256541@qq.com)
 */
public class RecyclableBatchWrapper implements Recyclable {
    private final List<KVState>       kvStateList;
    private final BloomFilter<byte[]> filter;

    public static RecyclableBatchWrapper newInstance() {
        return recyclers.get();
    }

    @Override
    public boolean recycle() {
        this.kvStateList.clear();
        this.filter.clear();
        return recyclers.recycle(this, handle);
    }

    private RecyclableBatchWrapper(final Recyclers.Handle handle) {
        this.handle = handle;
        this.filter = new BloomFilter<>(0.1, 10000);
        this.kvStateList = new ArrayList<>();
    }

    public BloomFilter<byte[]> getFilter() {
        return filter;
    }

    public List<KVState> getKvStateList() {
        return kvStateList;
    }

    private transient final Recyclers.Handle               handle;

    private static final Recyclers<RecyclableBatchWrapper> recyclers = new Recyclers<RecyclableBatchWrapper>(256) {
                                                                         @Override
                                                                         protected RecyclableBatchWrapper newObject(final Handle handle) {
                                                                             return new RecyclableBatchWrapper(handle);
                                                                         }
                                                                     };
}
