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
package com.alipay.sofa.jraft.rhea.storage;

import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;

/**
 * for test
 *
 * @author jiachun.fjc
 */
public class LocalLock extends DistributedLock<byte[]> {

    private final RawKVStore rawKVStore;

    public LocalLock(byte[] target, long lease, TimeUnit unit, RawKVStore rawKVStore) {
        super(target, lease, unit, null);
        this.rawKVStore = rawKVStore;
    }

    @Override
    public void unlock() {
        final byte[] internalKey = getInternalKey();
        final Acquirer acquirer = getAcquirer();
        this.rawKVStore.releaseLockWith(internalKey, acquirer, null);
    }

    @Override
    protected Owner internalTryLock(final byte[] ctx) {
        final byte[] internalKey = getInternalKey();
        final Acquirer acquirer = getAcquirer();
        acquirer.setContext(ctx);
        final KVStoreClosure closure = new TestClosure();
        this.rawKVStore.tryLockWith(internalKey, internalKey, false, acquirer, closure);
        final Owner owner = (Owner) closure.getData();
        updateOwnerAndAcquirer(owner);
        return owner;
    }
}
