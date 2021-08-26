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

import com.alipay.remoting.util.StringUtils;
import com.alipay.sofa.jraft.rhea.fsm.ParallelPipeline.KvEvent;
import com.alipay.sofa.jraft.rhea.fsm.dag.DagGraph;
import com.alipay.sofa.jraft.rhea.fsm.dag.GraphNode;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.lmax.disruptor.WorkHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Detect dependencies between current task and all pre tasks
 * @author hzh (642256541@qq.com)
 */
public class DetectDependencyHandler implements WorkHandler<KvEvent> {

    private final DagGraph<RecyclableKvTask> dagGraph;
    private final List<Detector>             detectors;

    public DetectDependencyHandler(final DagGraph<RecyclableKvTask> dagGraph) {
        this.dagGraph = dagGraph;
        this.detectors = new ArrayList<Detector>() {
            {
                add(new RegionRelatedDetector());
                add(new RangeRelatedDetector());
                add(new BloomDetector());
            }
        };
    }

    @Override
    public void onEvent(final KvEvent event) {
        final RecyclableKvTask curTask = event.getTask();
        final List<GraphNode<RecyclableKvTask>> preNodes = this.dagGraph.getAllNodes();
        final List<GraphNode<RecyclableKvTask>> dependentNodes = new ArrayList<>(preNodes.size());
        for (final GraphNode<RecyclableKvTask> preNode : preNodes) {
            if (preNode.isDone()) {
                continue;
            }
            if (doDetect(preNode.getItem(), curTask)) {
                dependentNodes.add(preNode);
            }
        }
        if (dependentNodes.size() > 0) {
            System.out.println("dependent happen");
        }
        final GraphNode<RecyclableKvTask> node = new GraphNode<>(curTask);
        this.dagGraph.addNode(node, dependentNodes);
    }

    public boolean doDetect(final RecyclableKvTask parentTask, final RecyclableKvTask childTask) {
        for (final Detector detector : this.detectors) {
            if (detector.doDetect(parentTask, childTask)) {
                return true;
            }
        }
        return false;
    }

    public interface Detector {

        /**
         * Detect whether childTask depends on parentTask
         * @return true if have dependencies
         */
        boolean doDetect(final RecyclableKvTask parentTask, final RecyclableKvTask childTask);
    }

    /**
     * Detect dependencies by region-operations : scan, merge
     */
    public static class RegionRelatedDetector implements Detector {
        @Override
        public boolean doDetect(final RecyclableKvTask parentTask, final RecyclableKvTask childTask) {
            // todo: how to deal with merge or split ? I think it cannot be handled
            // so just return true if there exists region-related operations
            if (parentTask.hasRegionOperation() || childTask.hasRegionOperation()) {
                return true;
            }
            return false;
        }
    }

    /**
     *
     * Detect dependencies by range-operations : scan, merge, delete_range
     */
    public static class RangeRelatedDetector implements Detector {
        @Override
        public boolean doDetect(final RecyclableKvTask parentTask, final RecyclableKvTask childTask) {
            // Check whether the intervals have intersections
            if (childTask.hasRangeOperation()) {
                final String maxKey = parentTask.getMaxKey();
                final String minKey = parentTask.getMinKey();
                if (StringUtils.isEmpty(minKey) || StringUtils.isEmpty(maxKey)) {
                    return false;
                }
                final List<Pair<String, String>> rangeKeyPairList = childTask.getRangeKeyPairList();
                // Range pair : key = range_start, value = range_end
                for (final Pair<String, String> keyPair : rangeKeyPairList) {
                    if (!(keyPair.getKey().compareTo(maxKey) > 0 || keyPair.getValue().compareTo(minKey) < 0)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     *
     * Detect dependencies by bloomFilter
     */
    public static class BloomDetector implements Detector {

        @Override
        public boolean doDetect(final RecyclableKvTask parentTask, final RecyclableKvTask childTask) {
            final BloomFilter parentBloomFilter = parentTask.getFilter();
            final BloomFilter childBloomFilter = childTask.getFilter();
            // Check whether have same key
            return parentBloomFilter.intersects(childBloomFilter);
        }
    }
}
