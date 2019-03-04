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

/*
 * 
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2007-2008 Sun Microsystems, Inc. All rights reserved.
 * 
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common Development
 * and Distribution License("CDDL") (collectively, the "License").  You
 * may not use this file except in compliance with the License. You can obtain
 * a copy of the License at https://glassfish.dev.java.net/public/CDDL+GPL.html
 * or glassfish/bootstrap/legal/LICENSE.txt.  See the License for the specific
 * language governing permissions and limitations under the License.
 * 
 * When distributing the software, include this License Header Notice in each
 * file and include the License file at glassfish/bootstrap/legal/LICENSE.txt.
 * Sun designates this particular file as subject to the "Classpath" exception
 * as provided by Sun in the GPL Version 2 section of the License file that
 * accompanied this code.  If applicable, add the following below the License
 * Header, with the fields enclosed by brackets [] replaced by your own
 * identifying information: "Portions Copyrighted [year]
 * [name of copyright owner]"
 * 
 * Contributor(s):
 * 
 * If you wish your version of this file to be governed by only the CDDL or
 * only the GPL Version 2, indicate your decision by adding "[Contributor]
 * elects to include this software in this distribution under the [CDDL or GPL
 * Version 2] license."  If you don't indicate a single choice of license, a
 * recipient has the option to distribute your version of this file under
 * either the CDDL, the GPL Version 2 or to extend the choice of license to
 * its licensees as provided above.  However, if you add GPL Version 2 code
 * and therefore, elected the GPL Version 2 license, then the option applies
 * only if the new code is made subject to such option by the copyright
 * holder.
 *
 */

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple {@link Future} implementation, which uses {@link ReentrantLock} to
 * synchronize during the lifecycle.
 * 
 * @see Future
 * @see ReentrantLock
 * 
 * @author Alexey Stashok
 */
public class FutureImpl<R> implements Future<R> {

    protected final ReentrantLock lock;

    protected boolean             isDone;

    protected CountDownLatch      latch;

    protected boolean             isCancelled;
    protected Throwable           failure;

    protected R                   result;

    public FutureImpl() {
        this(new ReentrantLock());
    }

    public FutureImpl(ReentrantLock lock) {
        this.lock = lock;
        latch = new CountDownLatch(1);
    }

    /**
     * Get current result value without any blocking.
     * 
     * @return current result value without any blocking.
     */
    public R getResult() {
        try {
            lock.lock();
            return result;
        } finally {
            lock.unlock();
        }
    }

    public Throwable getFailure() {
        try {
            lock.lock();
            return this.failure;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set the result value and notify about operation completion.
     * 
     * @param result
     *            the result value
     */
    public void setResult(R result) {
        try {
            lock.lock();
            this.result = result;
            notifyHaveResult();
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        try {
            lock.lock();
            isCancelled = true;
            notifyHaveResult();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCancelled() {
        try {
            lock.lock();
            return isCancelled;
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        try {
            lock.lock();
            return isDone;
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public R get() throws InterruptedException, ExecutionException {
        latch.await();

        try {
            lock.lock();
            if (isCancelled) {
                throw new CancellationException();
            } else if (failure != null) {
                throw new ExecutionException(failure);
            }

            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        final boolean isTimeOut = !latch.await(timeout, unit);
        try {
            lock.lock();
            if (!isTimeOut) {
                if (isCancelled) {
                    throw new CancellationException();
                } else if (failure != null) {
                    throw new ExecutionException(failure);
                }

                return result;
            } else {
                throw new TimeoutException();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Notify about the failure, occured during asynchronous operation
     * execution.
     */
    public void failure(Throwable failure) {
        try {
            lock.lock();
            this.failure = failure;
            notifyHaveResult();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Notify blocked listeners threads about operation completion.
     */
    protected void notifyHaveResult() {
        isDone = true;
        latch.countDown();
    }
}