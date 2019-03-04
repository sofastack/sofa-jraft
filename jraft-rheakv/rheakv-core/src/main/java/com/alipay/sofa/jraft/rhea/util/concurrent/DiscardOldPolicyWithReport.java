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
package com.alipay.sofa.jraft.rhea.util.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * A handler for rejected tasks that discards the oldest unhandled
 * request and then retries {@code execute}, unless the executor
 * is shut down, in which case the task is discarded.
 *
 * @author jiachun.fjc
 */
public class DiscardOldPolicyWithReport extends AbstractRejectedExecutionHandler {

    public DiscardOldPolicyWithReport(String threadPoolName) {
        super(threadPoolName, false, "");
    }

    public DiscardOldPolicyWithReport(String threadPoolName, String dumpPrefixName) {
        super(threadPoolName, true, dumpPrefixName);
    }

    @Override
    public void rejectedExecution(final Runnable r, final ThreadPoolExecutor e) {
        LOG.error("Thread pool [{}] is exhausted! {}.", threadPoolName, e.toString());

        dumpJvmInfoIfNeeded();

        if (!e.isShutdown()) {
            final BlockingQueue<Runnable> queue = e.getQueue();
            int discardSize = queue.size() >> 1;
            discardSize = discardSize <= 0 ? 1 : discardSize;
            for (int i = 0; i < discardSize; i++) {
                queue.poll();
            }
            e.execute(r);
        }
    }
}
