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
package com.alipay.sofa.jraft.entity;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.closure.JoinableClosure;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Basic message structure of jraft, contains:
 * <ul>
 * <li>data: associated  task data</li>
 * <li>done: task closure, called when the data is successfully committed to the raft group.</li>
 * <li>expectedTerm: Reject this task if expectedTerm doesn't match the current term of this Node if the value is not -1, default is -1.</li>
 * </ul>
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-13 3:08:12 PM
 */
public class Task implements Serializable {

    private static final long serialVersionUID = 2971309899898274575L;

    /** Associated  task data*/
    private ByteBuffer        data             = LogEntry.EMPTY_DATA;
    /** task closure, called when the data is successfully committed to the raft group or failures happen.*/
    private Closure           done;
    /** Reject this task if expectedTerm doesn't match the current term of this Node if the value is not -1, default is -1.*/
    private long              expectedTerm     = -1;

    public Task() {
        super();
    }

    /**
     * Creates a task with data/done.
     */
    public Task(final ByteBuffer data, final Closure done) {
        super();
        this.data = data;
        this.done = done;
    }

    /**
     * Creates a task with data/done/expectedTerm.
     */
    public Task(final ByteBuffer data, final Closure done, final long expectedTerm) {
        super();
        this.data = data;
        this.done = done;
        this.expectedTerm = expectedTerm;
    }

    public ByteBuffer getData() {
        return this.data;
    }

    public void setData(final ByteBuffer data) {
        Requires.requireNonNull(data, "data should not be null, you can use LogEntry.EMPTY_DATA instead.");
        this.data = data;
    }

    public Closure getDone() {
        return this.done;
    }

    public void setDone(final Closure done) {
        this.done = done;
    }

    public long getExpectedTerm() {
        return this.expectedTerm;
    }

    public void setExpectedTerm(final long expectedTerm) {
        this.expectedTerm = expectedTerm;
    }

    /**
     * Waiting for the task to complete, to note that throughput may be reduced,
     * which is generally not recommended.
     *
     * @return done closure
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @since 1.3.1
     */
    public Closure join() throws InterruptedException {
        final JoinableClosure joinable = castToJoinalbe(this.done);
        joinable.join();
        return joinable.getClosure();
    }

    /**
     * Waiting for the task to complete with a timeout millis, to note that throughput
     * may be reduced, which is generally not recommended.
     *
     * @param timeoutMillis  the maximum millis to wait
     * @return done closure
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TimeoutException if timeout
     * @since 1.3.1
     */
    public Closure join(final long timeoutMillis) throws InterruptedException, TimeoutException {
        final JoinableClosure joinable = castToJoinalbe(this.done);
        joinable.join(timeoutMillis);
        return joinable.getClosure();
    }

    /**
     * Waiting for all tasks to complete.
     *
     * @param tasks task list
     * @return the closure list in tasks
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @since 1.3.1
     */
    public static List<Closure> joinAll(final List<Task> tasks) throws InterruptedException {
        final List<Closure> closures = new ArrayList<>(tasks.size());
        for (final Task t : tasks) {
            closures.add(t.join());
        }
        return closures;
    }

    /**
     * Waiting for all tasks to complete with a timeout millis.
     *
     * @param tasks         task list
     * @param timeoutMillis the maximum millis to wait
     * @return the closure list in the tasks
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TimeoutException if timeout
     * @since 1.3.1
     */
    public static List<Closure> joinAll(final List<Task> tasks, long timeoutMillis) throws InterruptedException,
                                                                                   TimeoutException {
        final List<Closure> closures = new ArrayList<>(tasks.size());
        for (final Task t : tasks) {
            final long start = System.nanoTime();
            closures.add(t.join(timeoutMillis));
            timeoutMillis -= TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            if (timeoutMillis <= 0) {
                throw new TimeoutException("joined timeout");
            }
        }
        return closures;
    }

    private static JoinableClosure castToJoinalbe(final Closure closure) {
        if (closure instanceof JoinableClosure) {
            return (JoinableClosure) closure;
        }
        throw new UnsupportedOperationException("Unsupported join");
    }
}
