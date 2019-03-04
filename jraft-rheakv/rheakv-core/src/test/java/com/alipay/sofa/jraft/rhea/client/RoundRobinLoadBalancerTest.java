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
package com.alipay.sofa.jraft.rhea.client;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alipay.sofa.jraft.rhea.util.Lists;

/**
 *
 * @author jiachun.fjc
 */
public class RoundRobinLoadBalancerTest {

    @Test
    public void selectTest() {
        List<Integer> elements = Lists.newArrayList();
        elements.add(0);
        elements.add(1);
        elements.add(2);
        elements.add(3);
        elements.add(4);
        RoundRobinLoadBalancer balancer1 = RoundRobinLoadBalancer.getInstance(1);
        Assert.assertEquals(0, balancer1.select(elements).intValue());
        Assert.assertEquals(1, balancer1.select(elements).intValue());

        RoundRobinLoadBalancer balancer2 = RoundRobinLoadBalancer.getInstance(2);
        Assert.assertEquals(0, balancer2.select(elements).intValue());
        Assert.assertEquals(1, balancer2.select(elements).intValue());
        Assert.assertEquals(2, balancer2.select(elements).intValue());
        Assert.assertEquals(3, balancer2.select(elements).intValue());
        Assert.assertEquals(4, balancer2.select(elements).intValue());
        Assert.assertEquals(0, balancer2.select(elements).intValue());
    }
}
