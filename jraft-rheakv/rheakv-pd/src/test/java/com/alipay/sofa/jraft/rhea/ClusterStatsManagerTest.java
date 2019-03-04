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
package com.alipay.sofa.jraft.rhea;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Pair;

/**
 * @author jiachun.fjc
 */
public class ClusterStatsManagerTest {

    @Before
    public void reset() {
        ClusterStatsManager.invalidCache();
    }

    @Test
    public void findModelWorkerStoresTest() {
        final ClusterStatsManager manager = ClusterStatsManager.getInstance(1);
        manager.addOrUpdateLeader(10, 101);
        manager.addOrUpdateLeader(10, 102);
        manager.addOrUpdateLeader(10, 103);
        manager.addOrUpdateLeader(11, 104);
        manager.addOrUpdateLeader(11, 105);
        manager.addOrUpdateLeader(12, 106);
        Pair<Set<Long>, Integer> result = manager.findModelWorkerStores(1);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.getKey().size());
        Assert.assertTrue(result.getKey().contains(10L));
        Assert.assertEquals(Integer.valueOf(3), result.getValue());

        manager.addOrUpdateLeader(11, 107);
        result = manager.findModelWorkerStores(1);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getKey().contains(10L));
        Assert.assertTrue(result.getKey().contains(11L));
        Assert.assertEquals(2, result.getKey().size());
        Assert.assertEquals(Integer.valueOf(3), result.getValue());

        manager.addOrUpdateLeader(11, 101);
        result = manager.findModelWorkerStores(2);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.getKey().size());
        Assert.assertEquals(Integer.valueOf(4), result.getValue());
    }

    @Test
    public void findLazyWorkerStoresTest() {
        final ClusterStatsManager manager = ClusterStatsManager.getInstance(1);
        manager.addOrUpdateLeader(10, 101);
        manager.addOrUpdateLeader(10, 102);
        manager.addOrUpdateLeader(10, 103);
        manager.addOrUpdateLeader(11, 104);
        manager.addOrUpdateLeader(11, 105);
        manager.addOrUpdateLeader(12, 106);
        final Collection<Long> storeCandidates = Lists.newArrayList();
        storeCandidates.add(10L);
        storeCandidates.add(11L);
        storeCandidates.add(12L);
        List<Pair<Long, Integer>> result = manager.findLazyWorkerStores(storeCandidates);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Long.valueOf(12), result.get(0).getKey());
        Assert.assertEquals(Integer.valueOf(1), result.get(0).getValue());

        manager.addOrUpdateLeader(10, 105);
        result = manager.findLazyWorkerStores(storeCandidates);
        Assert.assertNotNull(result);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(Integer.valueOf(1), result.get(0).getValue());
    }
}
