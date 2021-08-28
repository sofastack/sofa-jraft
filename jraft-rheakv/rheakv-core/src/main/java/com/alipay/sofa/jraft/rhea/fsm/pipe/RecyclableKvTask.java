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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.rhea.options.ParallelSmrOptions;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Recyclable;
import com.alipay.sofa.jraft.util.Recyclers;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author hzh (642256541@qq.com)
 */
public class RecyclableKvTask implements Recyclable {

    private static final int                 BatchSize = ParallelSmrOptions.TaskBatchSize;

    private static final int                 BloomSize = 1024 * 1024;

    // Bloom filter, map all the involved keys
    private final BloomFilter                filter;

    // Called when run done
    private Closure                          done;

    // Store all operations
    private final List<KVState>              kvStateList;

    // Store range-operations's key pair [startKye, endKey] , such as scan, delete-range....
    private final List<Pair<String, String>> rangeKeyPairList;

    // Store region-operations such as region-split, merge....
    private final List<KVState>              regionStateList;

    // The min key among all the involved keys
    private String                           minKey;

    // The max key among all the involved keys
    private String                           maxKey;

    public static RecyclableKvTask newInstance() {
        return recyclers.get();
    }

    @Override
    public boolean recycle() {
        this.kvStateList.clear();
        this.rangeKeyPairList.clear();
        this.regionStateList.clear();
        this.filter.clear();
        this.minKey = "";
        this.maxKey = "";
        this.done = null;
        return recyclers.recycle(this, handle);
    }

    private RecyclableKvTask(final Recyclers.Handle handle) {
        this.handle = handle;
        this.filter = new BloomFilter(BloomSize);
        this.kvStateList = new ArrayList<>(BatchSize);
        this.rangeKeyPairList = new ArrayList<>(BatchSize);
        this.regionStateList = new ArrayList<>(BatchSize);
        this.minKey = "";
        this.maxKey = "";
    }

    public void setDone(final Closure done) {
        this.done = done;
    }

    public Closure getDone() {
        return this.done;
    }

    public BloomFilter getFilter() {
        return this.filter;
    }

    public List<KVState> getKvStateList() {
        return this.kvStateList;
    }

    public void addRangeKeyPair(final Pair<String, String> range) {
        this.rangeKeyPairList.add(range);
    }

    public void addRegionOperation(final KVState kvState) {
        this.regionStateList.add(kvState);
    }

    public boolean hasRegionOperation() {
        return !this.regionStateList.isEmpty();
    }

    public boolean hasRangeOperation() {
        return !this.rangeKeyPairList.isEmpty();
    }

    /**
     *
     * Select the boundaryKey (maxKey, minKey) from keysList
     */
    public void updateBoundaryKey(final List<byte[]> keysList) {
        if (!keysList.isEmpty()) {
            final List<String> strKeyList = keysList.stream().map(BytesUtil::readUtf8)
                    .collect(Collectors.toList());
            int minIndex = 0, maxIndex = 0;
            for (int i = 1; i < strKeyList.size(); i++) {
                String key = strKeyList.get(i);
                if (key.compareTo(strKeyList.get(minIndex)) < 0) {
                    minIndex = i;
                }
                if (key.compareTo(strKeyList.get(maxIndex)) > 0) {
                    maxIndex = i;
                }
            }
            this.minKey = strKeyList.get(minIndex);
            this.maxKey = strKeyList.get(maxIndex);
        } else {
            this.minKey = this.maxKey = "";
        }
    }

    public String getMaxKey() {
        return this.maxKey;
    }

    public void setMaxKey(final String maxKey) {
        this.maxKey = maxKey;
    }

    public String getMinKey() {
        return this.minKey;
    }

    public void setMinKey(final String minKey) {
        this.minKey = minKey;
    }

    public List<Pair<String, String>> getRangeKeyPairList() {
        return rangeKeyPairList;
    }

    @Override
    public String toString() {
        return "RecyclableKvTask{" + "kvStateList=" + kvStateList + '}';
    }

    private transient final Recyclers.Handle         handle;

    private static final Recyclers<RecyclableKvTask> recyclers = new Recyclers<RecyclableKvTask>(256) {
                                                                   @Override
                                                                   protected RecyclableKvTask newObject(final Handle handle) {
                                                                       return new RecyclableKvTask(handle);
                                                                   }
                                                               };
}
