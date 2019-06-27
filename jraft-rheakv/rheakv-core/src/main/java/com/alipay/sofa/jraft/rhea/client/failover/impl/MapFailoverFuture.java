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
package com.alipay.sofa.jraft.rhea.client.failover.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.FutureGroup;
import com.alipay.sofa.jraft.rhea.client.failover.RetryCallable;
import com.alipay.sofa.jraft.rhea.errors.ApiExceptionHelper;
import com.alipay.sofa.jraft.rhea.util.Attachable;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;

/**
 * A helper object for map result failover, which is an immutable object.
 * A new object will be created when a retry operation occurs and
 * {@code retriesLeft} will decrease by 1, until {@code retriesLeft} == 0.
 *
 * @author jiachun.fjc
 */
public final class MapFailoverFuture<K, V> extends CompletableFuture<Map<K, V>> implements Attachable<Object> {

    private static final Logger            LOG = LoggerFactory.getLogger(MapFailoverFuture.class);

    private final int                      retriesLeft;
    private final RetryCallable<Map<K, V>> retryCallable;
    private final Object                   attachments;

    public MapFailoverFuture(int retriesLeft, RetryCallable<Map<K, V>> retryCallable) {
        this(retriesLeft, retryCallable, null);
    }

    public MapFailoverFuture(int retriesLeft, RetryCallable<Map<K, V>> retryCallable, Object attachments) {
        this.retriesLeft = retriesLeft;
        this.retryCallable = retryCallable;
        this.attachments = attachments;
    }

    @Override
    public boolean completeExceptionally(final Throwable ex) {
        if (this.retriesLeft > 0 && ApiExceptionHelper.isInvalidEpoch(ex)) {
            LOG.warn("[InvalidEpoch-Failover] cause: {}, [{}] retries left.", StackTraceUtil.stackTrace(ex),
                    this.retriesLeft);
            final FutureGroup<Map<K, V>> futureGroup = this.retryCallable.run(ex);
            CompletableFuture.allOf(futureGroup.toArray()).whenComplete((ignored, throwable) -> {
                if (throwable == null) {
                    final Map<K, V> all = Maps.newHashMap();
                    for (final CompletableFuture<Map<K, V>> partOf : futureGroup.futures()) {
                        all.putAll(partOf.join());
                    }
                    super.complete(all);
                } else {
                    super.completeExceptionally(throwable);
                }
            });
            return false;
        }
        if (this.retriesLeft <= 0) {
            LOG.error("[InvalidEpoch-Failover] cause: {}, {} retries left.", StackTraceUtil.stackTrace(ex),
                    this.retriesLeft);
        }
        return super.completeExceptionally(ex);
    }

    @Override
    public Object getAttachments() {
        return attachments;
    }
}
