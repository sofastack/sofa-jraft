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
package com.alipay.sofa.jraft.rpc.impl;

import java.util.concurrent.TimeUnit;

import io.grpc.Server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jiachun.fjc
 */
public class GrpcServerHelper {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcServerHelper.class);

    /**
     * @see #shutdownAndAwaitTermination(Server, long)
     */
    public static boolean shutdownAndAwaitTermination(final Server server) {
        return shutdownAndAwaitTermination(server, 1000);
    }

    /**
     * The following method shuts down an {@code Server} in two
     * phases, first by calling {@code shutdown} to reject incoming tasks,
     * and then calling {@code shutdownNow}, if necessary, to cancel any
     * lingering tasks.
     */
    public static boolean shutdownAndAwaitTermination(final Server server, final long timeoutMillis) {
        if (server == null) {
            return true;
        }
        // disable new tasks from being submitted
        server.shutdown();
        final TimeUnit unit = TimeUnit.MILLISECONDS;
        final long phaseOne = timeoutMillis / 5;
        try {
            // wait a while for existing tasks to terminate
            if (server.awaitTermination(phaseOne, unit)) {
                return true;
            }
            server.shutdownNow();
            // wait a while for tasks to respond to being cancelled
            if (server.awaitTermination(timeoutMillis - phaseOne, unit)) {
                return true;
            }
            LOG.warn("Fail to shutdown grpc server: {}.", server);
        } catch (final InterruptedException e) {
            // (Re-)cancel if current thread also interrupted
            server.shutdownNow();
            // preserve interrupt status
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
