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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.util.BloomFilter;
import com.alipay.sofa.jraft.util.Recyclable;
import com.alipay.sofa.jraft.util.Recyclers;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hzh (642256541@qq.com)
 */
public class RecyclableKvTask implements Recyclable {

    /**
     * Task status
     */
    public enum TaskStatus {
        INIT, // On the pipeline
        WAITING, // On the dag graph, waited to be scheduled
        RUNNING, // Running
        DONE // Already schedule done
    }

    private volatile TaskStatus       taskStatus = TaskStatus.INIT;
    private final List<KVState>       kvStateList;
    private final BloomFilter<byte[]> filter;
    private Closure                   done;

    // Whether this batch has op like merge / split
    private volatile boolean          hasRegionRelatedOp;
    // Whether this batch has op like scan
    private volatile boolean          hasScanRelatedOp;

    public static RecyclableKvTask newInstance() {
        return recyclers.get();
    }

    @Override
    public boolean recycle() {
        this.kvStateList.clear();
        this.filter.clear();
        this.done = null;
        this.taskStatus = TaskStatus.INIT;
        this.hasRegionRelatedOp = false;
        this.hasScanRelatedOp = false;
        return recyclers.recycle(this, handle);
    }

    private RecyclableKvTask(final Recyclers.Handle handle) {
        this.handle = handle;
        // TODO: Move the parameters to Options
        this.filter = new BloomFilter<>(0.1, 10000);
        this.kvStateList = new ArrayList<>();
    }

    public void setHasRegionRelatedOp(final boolean hasRegionRelatedOp) {
        this.hasRegionRelatedOp = hasRegionRelatedOp;
    }

    public void setDone(final Closure done) {
        this.done = done;
    }

    public void setHasScanRelatedOp(final boolean hasScanRelatedOp) {
        this.hasScanRelatedOp = hasScanRelatedOp;
    }

    public Closure getDone() {
        return done;
    }

    public BloomFilter<byte[]> getFilter() {
        return filter;
    }

    public List<KVState> getKvStateList() {
        return kvStateList;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(final TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
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
