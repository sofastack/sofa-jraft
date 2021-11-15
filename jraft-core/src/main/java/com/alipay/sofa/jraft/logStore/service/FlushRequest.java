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
package com.alipay.sofa.jraft.logStore.service;

import java.util.concurrent.CompletableFuture;

/**
 * @author hzh (642256541@qq.com)
 */
public class FlushRequest {

    private final long                       expectedFlushPosition;

    private final CompletableFuture<Boolean> future;

    public FlushRequest(final long expectedFlushPosition, final CompletableFuture<Boolean> future) {
        this.expectedFlushPosition = expectedFlushPosition;
        this.future = future;
    }

    public static FlushRequest buildRequest(final long expectedFlushPosition) {
        return new FlushRequest(expectedFlushPosition, new CompletableFuture<>());
    }

    public CompletableFuture<Boolean> getFuture() {
        return future;
    }

    public long getExpectedFlushPosition() {
        return expectedFlushPosition;
    }

    // Wakeup future
    public void wakeup() {
        this.future.complete(true);
    }

}
