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
package com.alipay.sofa.jraft.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Replicator id with lock.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-29 10:59:47 AM
 */
public class ThreadId {

    private final Object              data;
    private volatile NonReentrantLock lock          = new NonReentrantLock();
    private final List<Integer>       pendingErrors = Collections.synchronizedList(new ArrayList<>());
    private final OnError             onError;

    /**
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Mar-29 11:01:54 AM
     */
    public interface OnError {
        void onError(ThreadId id, Object data, int errorCode);
    }

    public ThreadId(Object data, OnError onError) {
        super();
        this.data = data;
        this.onError = onError;
    }

    public Object getData() {
        return data;
    }

    public Object lock() {
        final Lock theLock = this.lock;
        if (theLock == null) {
            return null;
        }
        try {
            while (!theLock.tryLock(1, TimeUnit.SECONDS)) {
                if (this.lock == null) {
                    return null;
                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); //reset
            return null;
        }
        //Got the lock, double checking state.
        if (this.lock == null) {
            //should release lock
            theLock.unlock();
            return null;
        }
        return this.data;
    }

    public void unlock() {
        final Lock theLock = this.lock;
        if (theLock == null) {
            return;
        }
        // calls all pending errors before unlock
        try {
            final List<Integer> errors;
            synchronized (this.pendingErrors) {
                errors = new ArrayList<>(this.pendingErrors);
                this.pendingErrors.clear();
            }
            for (final Integer code : errors) {
                if (this.onError != null) {
                    this.onError.onError(this, this.data, code);
                }
            }
        } finally {
            doUnlock(theLock);
        }
    }

    private void doUnlock(Lock theLock) {
        if (theLock != null) {
            try {
                theLock.unlock();
            } catch (final Exception e) {
                //ignore
            }
        }
    }

    public void join() {
        while (this.lock != null) {
            Thread.yield();
        }
    }

    @Override
    public String toString() {
        return this.data.toString();
    }

    public void unlockAndDestroy() {
        final Lock theLock = this.lock;
        this.lock = null;
        if (theLock != null) {
            try {
                theLock.unlock();
            } catch (final Exception ignored) {
                // ignored
            }
        }
    }

    /**
     * Set error code, if it tryLock success, run the onError callback
     * with code immediately, else add it into pending errors and will
     * be called before unlock.
     *
     * @param errorCode error code
     */
    public void setError(int errorCode) {
        final Lock theLock = this.lock;
        if (theLock == null) {
            return;
        }
        if (theLock.tryLock()) {
            try {
                if (this.onError != null) {
                    this.onError.onError(this, data, errorCode);
                }
            } finally {
                doUnlock(theLock);
            }
        } else {
            this.pendingErrors.add(errorCode);
        }
    }
}
