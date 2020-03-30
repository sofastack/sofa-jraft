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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.alipay.sofa.jraft.rhea.errors.NeverGetHereException;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;

/**
 *
 * @author jiachun.fjc
 */
public final class FutureHelper {

    public static final long DEFAULT_TIMEOUT_MILLIS = SystemPropertyUtil.getLong("rhea.default_future_timeout", 10000);

    public static <V> V get(final CompletableFuture<V> future) {
        return get(future, DEFAULT_TIMEOUT_MILLIS);
    }

    public static <V> V get(final CompletableFuture<V> future, final long timeoutMillis) {
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException e) {
            ThrowUtil.throwException(e);
        }
        throw NeverGetHereException.INSTANCE;
    }

    public static CompletableFuture<Boolean> joinBooleans(final FutureGroup<Boolean> futureGroup) {
        return joinBooleans(futureGroup, new CompletableFuture<>());
    }

    public static CompletableFuture<Boolean> joinBooleans(final FutureGroup<Boolean> futureGroup,
                                                          final CompletableFuture<Boolean> future) {
        CompletableFuture.allOf(futureGroup.toArray()).whenComplete((ignored, throwable) -> {
            if (throwable == null) {
                for (final CompletableFuture<Boolean> partOf : futureGroup.futures()) {
                    if (!partOf.join()) {
                        future.complete(false);
                        return;
                    }
                }
                future.complete(true);
            } else {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    public static <T> CompletableFuture<List<T>> joinList(final FutureGroup<List<T>> futureGroup) {
        return joinList(futureGroup, 0);
    }

    public static <T> CompletableFuture<List<T>> joinList(final FutureGroup<List<T>> futureGroup, final int size) {
        return joinList(futureGroup, size, new CompletableFuture<>());
    }

    public static <T> CompletableFuture<List<T>> joinList(final FutureGroup<List<T>> futureGroup, final int size,
                                                          final CompletableFuture<List<T>> future) {
        CompletableFuture.allOf(futureGroup.toArray()).whenComplete((ignored, throwable) -> {
            if (throwable == null) {
                final List<T> allResult = size > 0 ? Lists.newArrayListWithCapacity(size) : Lists.newArrayList();
                for (final CompletableFuture<List<T>> partOf : futureGroup.futures()) {
                    allResult.addAll(partOf.join());
                }
                future.complete(allResult);
            } else {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    public static <K, V> CompletableFuture<Map<K, V>> joinMap(final FutureGroup<Map<K, V>> futureGroup) {
        return joinMap(futureGroup, 0);
    }

    public static <K, V> CompletableFuture<Map<K, V>> joinMap(final FutureGroup<Map<K, V>> futureGroup, final int size) {
        return joinMap(futureGroup, size, new CompletableFuture<>());
    }

    public static <K, V> CompletableFuture<Map<K, V>> joinMap(final FutureGroup<Map<K, V>> futureGroup, final int size,
                                                              final CompletableFuture<Map<K, V>> future) {
        CompletableFuture.allOf(futureGroup.toArray()).whenComplete((ignored, throwable) -> {
            if (throwable == null) {
                final Map<K, V> allResult = size > 0 ? Maps.newHashMapWithExpectedSize(size) : Maps.newHashMap();
                for (final CompletableFuture<Map<K, V>> partOf : futureGroup.futures()) {
                    allResult.putAll(partOf.join());
                }
                future.complete(allResult);
            } else {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    private FutureHelper() {
    }
}
