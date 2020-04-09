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
package com.alipay.sofa.jraft.test.atomic.client;

import java.util.concurrent.CyclicBarrier;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;

public class AtomicClientTest {

    public static void main(String[] args) throws Exception {
        final RouteTable table = RouteTable.getInstance();
        table.updateConfiguration("atomic_0",
            JRaftUtils.getConfiguration("127.0.0.1:8609,127.0.0.1:8610,127.0.0.1:8611"));
        final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());
        final Status st = table.refreshLeader(cliClientService, "atomic_0", 10000);
        System.out.println(st);

        final AtomicClient cli = new AtomicClient("atomic", JRaftUtils.getConfiguration("localhost:8610"));
        final PeerId leader = table.selectLeader("atomic_0");
        cli.start();

        final int threads = 30;
        final int count = 10000;
        final CyclicBarrier barrier = new CyclicBarrier(threads + 1);

        for (int t = 0; t < threads; t++) {
            new Thread() {
                @Override
                public void run() {
                    long sum = 0;
                    try {
                        barrier.await();
                        final PeerId peer = new PeerId("localhost", 8611);
                        for (int i = 0; i < count; i++) {
                            sum += cli.get(leader, "a", true, false);
                            //sum += cli.addAndGet(leader, "a", i);
                        }
                        barrier.await();
                    } catch (final Exception e) {
                        e.printStackTrace();
                    } finally {
                        System.out.println("sum=" + sum);
                    }
                }
            }.start();

        }
        final long start = System.currentTimeMillis();
        barrier.await();
        barrier.await();
        final long cost = System.currentTimeMillis() - start;
        final long tps = Math.round(threads * count * 1000.0 / cost);
        System.out.println("tps=" + tps + ",cost=" + cost + " ms.");
        cli.shutdown();
    }
}
