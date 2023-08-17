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
package com.alipay.sofa.jraft.option;

import com.codahale.metrics.MetricRegistry;
import org.junit.Test;

import static org.junit.Assert.*;

public class NodeOptionsTest {

    @Test
    public void testCopyRpcOptionsRight() {
        final NodeOptions nodeOptions = new NodeOptions();

        assertEquals(1000, nodeOptions.getRpcConnectTimeoutMs());
        assertEquals(5000, nodeOptions.getRpcDefaultTimeout());
        assertEquals(5 * 60 * 1000, nodeOptions.getRpcInstallSnapshotTimeout());
        assertEquals(80, nodeOptions.getRpcProcessorThreadPoolSize());
        assertFalse(nodeOptions.isEnableRpcChecksum());
        assertNull(nodeOptions.getMetricRegistry());

        //change options
        nodeOptions.setRpcConnectTimeoutMs(2000);
        nodeOptions.setRpcDefaultTimeout(6000);
        nodeOptions.setRpcInstallSnapshotTimeout(6 * 60 * 1000);
        nodeOptions.setRpcProcessorThreadPoolSize(90);
        nodeOptions.setEnableRpcChecksum(true);
        nodeOptions.setMetricRegistry(new MetricRegistry());

        //copy options
        final NodeOptions copy = nodeOptions.copy();

        assertEquals(2000, copy.getRpcConnectTimeoutMs());
        assertEquals(6000, copy.getRpcDefaultTimeout());
        assertEquals(6 * 60 * 1000, copy.getRpcInstallSnapshotTimeout());
        assertEquals(90, copy.getRpcProcessorThreadPoolSize());
        assertTrue(copy.isEnableRpcChecksum());
        assertNotNull(copy.getMetricRegistry());
    }
}