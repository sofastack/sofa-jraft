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
package com.alipay.sofa.jraft.rhea.pd;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;

import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeValue;

/**
 *
 * @author jiachun.fjc
 */
public class RheaHeartbeatTest extends RheaKVTestCluster {

    private static final NamedThreadFactory threadFactory = new NamedThreadFactory("heartbeat_test", true);

    public static void main(String[] args) throws Exception {
        final RheaHeartbeatTest heartbeatTest = new RheaHeartbeatTest();
        heartbeatTest.start();

        final Thread thread = threadFactory.newThread(() -> {
            for (;;) {
                heartbeatTest.putAndGetValue();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                heartbeatTest.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

    private void putAndGetValue() {
        final RheaKVStore store = getLeaderStore(ThreadLocalRandom.current().nextInt(1, 2));
        final String key = UUID.randomUUID().toString();
        final byte[] value = makeValue(UUID.randomUUID().toString());
        store.bPut(key, value);
        final byte[] newValue = store.bGet(key);
        Assert.assertArrayEquals(value, newValue);
    }
}
