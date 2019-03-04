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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.alipay.sofa.jraft.rhea.client.failover.FailoverClosure;
import com.alipay.sofa.jraft.rhea.client.failover.RetryCallable;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.client.failover.impl.FailoverClosureImpl;
import com.alipay.sofa.jraft.rhea.client.failover.impl.MapFailoverFuture;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;

/**
 * @author jiachun.fjc
 */
public class MapFailoverFutureTest {

    private final ConcurrentMap<String, AtomicInteger> counters = new ConcurrentHashMap<>();

    @Test
    public void setFailureTest() throws ExecutionException, InterruptedException {
        final int reties = 5;
        final int start = 1;
        final int end = 20;
        final FutureGroup<Map<Integer, Integer>> futureGroup = multiGet(start, end, reties, null);
        final Map<Integer, Integer> resultList = Maps.newHashMap();
        for (final CompletableFuture<Map<Integer, Integer>> future : futureGroup.futures()) {
            resultList.putAll(FutureHelper.get(future));
        }
        final List<Integer> expectedList = new ArrayList<>();
        for (int i = start; i < end; i++) {
            expectedList.add(i);
        }
        Assert.assertArrayEquals(expectedList.toArray(), resultList.keySet().toArray());
        System.out.println("all result=" + resultList);
    }

    private FutureGroup<Map<Integer, Integer>> multiGet(final int start, final int end, final int retriesLeft,
                                                        final Throwable lastCause) {
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        final List<CompletableFuture<Map<Integer, Integer>>> futureList = Lists.newArrayList();
        final int mid = ((end - start) / 2) + start;
        for (int i = 0; i < 2; i++) {
            final int subStart = i == 0 ? start : mid;
            final int subEnd = i == 0 ? mid : end;
            if (subEnd - subStart > 0) {
                final RetryCallable<Map<Integer, Integer>> retryCallable = retryCause -> multiGet(subStart, subEnd,
                        retriesLeft - 1, retryCause);
                final MapFailoverFuture<Integer, Integer> future = new MapFailoverFuture<>(retriesLeft, retryCallable);
                regionMultiGet(subStart, subEnd, future, retriesLeft, lastError);
                futureList.add(future);
            }
        }
        return new FutureGroup<>(futureList);
    }

    @SuppressWarnings("unused")
    private void regionMultiGet(final int start, final int end, final CompletableFuture<Map<Integer, Integer>> future,
                                final int retriesLeft, final Errors lastCause) {
        System.out.println("start=" + start + ", end=" + end);
        final RetryRunner retryRunner = retryCause -> regionMultiGet(start, end, future, retriesLeft - 1,
                retryCause);
        final FailoverClosure<Map<Integer, Integer>> closure = new FailoverClosureImpl<>(future, false,
                retriesLeft, retryRunner);
        if (getCounter(start, end).incrementAndGet() < 2) {
            System.err.println("fail: " + start + " - " + end);
            closure.failure(Errors.INVALID_REGION_MEMBERSHIP);
        } else {
            final Map<Integer, Integer> result = Maps.newHashMap();
            for (int i = start; i < end; i++) {
                result.put(i, i);
            }
            closure.success(result);
        }
    }

    private AtomicInteger getCounter(final int start, final int end) {
        final String key = start + "_" + end;
        AtomicInteger counter = this.counters.get(key);
        if (counter == null) {
            AtomicInteger newCounter = new AtomicInteger();
            counter = this.counters.putIfAbsent(key, newCounter);
            if (counter == null) {
                counter = newCounter;
            }
        }
        return counter;
    }
}
