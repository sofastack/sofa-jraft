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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.alipay.sofa.jraft.util.Requires;

/**
 *
 * @author jiachun.fjc
 */
public class FutureGroup<V> extends CompletableFuture<V> {

    private final List<CompletableFuture<V>> futures;

    private volatile CompletableFuture<V>[]  array;

    public FutureGroup(List<CompletableFuture<V>> futures) {
        this.futures = Requires.requireNonNull(futures, "futures");
    }

    public List<CompletableFuture<V>> futures() {
        return futures;
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<V>[] toArray() {
        if (this.array == null) {
            synchronized (this) {
                if (this.array == null) {
                    final CompletableFuture<V>[] temp = new CompletableFuture[this.futures.size()];
                    this.futures.toArray(temp);
                    this.array = temp;
                }
            }
        }
        return this.array;
    }

    public int size() {
        return this.futures.size();
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        boolean result = true;
        for (final CompletableFuture<V> f : this.futures) {
            result = result && f.cancel(mayInterruptIfRunning);
        }
        return result;
    }

    @Override
    public boolean isCancelled() {
        for (final CompletableFuture<V> f : this.futures) {
            if (!f.isCancelled()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isDone() {
        for (final CompletableFuture<V> f : this.futures) {
            if (!f.isDone()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException("get");
    }

    @Override
    public V get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException,
                                                         TimeoutException {
        throw new UnsupportedOperationException("get");
    }
}
