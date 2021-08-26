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
package com.alipay.sofa.jraft.rhea.fsm;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.StoreEngine;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.ParallelSmrOptions;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVState;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Rhea KV parallel statemachine, use pipeline + disruptor + dagGraph
 * see https://github.com/sofastack/sofa-jraft/issues/614
 *
 * @author hzh (642256541@qq.com)
 */
public class ParallelKVStateMachine extends BaseKVStateMachine implements Lifecycle<ParallelSmrOptions> {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelKVStateMachine.class);

    private ParallelPipeline    parallelPipeline;

    public ParallelKVStateMachine(Region region, StoreEngine storeEngine) {
        super(region, storeEngine);
    }

    @Override
    public boolean init(final ParallelSmrOptions opts) {
        this.parallelPipeline = new ParallelPipeline(this, this.rawKVStore);
        this.parallelPipeline.init(opts);
        return true;
    }

    @Override
    public void shutdown() {
        this.parallelPipeline.shutdown();
    }

    @Override
    public void onApply(final Iterator iter) {
        // Put iter to pipeline and wait to be scheduled
        this.parallelPipeline.dispatchBatch(iter);
    }

    public void doSplit(final KVState kvState) {
        final byte[] parentKey = this.region.getStartKey();
        final KVOperation op = kvState.getOp();
        final long currentRegionId = op.getCurrentRegionId();
        final long newRegionId = op.getNewRegionId();
        final byte[] splitKey = op.getKey();
        final KVStoreClosure closure = kvState.getDone();
        try {
            this.rawKVStore.initFencingToken(parentKey, splitKey);
            this.storeEngine.doSplit(currentRegionId, newRegionId, splitKey);
            if (closure != null) {
                // null on follower
                closure.setData(Boolean.TRUE);
                closure.run(Status.OK());
            }
        } catch (final Throwable t) {
            LOG.error("Fail to split, regionId={}, newRegionId={}, splitKey={}.", currentRegionId, newRegionId,
                BytesUtil.toHex(splitKey));
            setCriticalError(closure, t);
        }
    }
}
