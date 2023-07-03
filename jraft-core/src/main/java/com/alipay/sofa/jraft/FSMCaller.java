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
package com.alipay.sofa.jraft;

import com.alipay.sofa.jraft.closure.LoadSnapshotClosure;
import com.alipay.sofa.jraft.closure.SaveSnapshotClosure;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.util.Describer;

/**
 * Finite state machine caller.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 11:07:52 AM
 */
public interface FSMCaller extends Lifecycle<FSMCallerOptions>, Describer {

    /**
     * Listen on lastAppliedLogIndex update events.
     *
     * @author dennis
     */
    interface LastAppliedLogIndexListener {

        /**
         * Called when lastAppliedLogIndex updated.
         *
         * @param lastAppliedLogIndex the log index of last applied
         */
        void onApplied(final long lastAppliedLogIndex);
    }

    /**
     * Returns true when current thread is the thread that calls state machine callback methods.
     * @return
     */
    boolean isRunningOnFSMThread();

    /**
     * Adds a LastAppliedLogIndexListener.
     */
    void addLastAppliedLogIndexListener(final LastAppliedLogIndexListener listener);

    /**
     * Called when log entry committed
     *
     * @param committedIndex committed log index
     */
    boolean onCommitted(final long committedIndex);

    /**
     * Given specified <tt>requiredCapacity</tt> determines if that amount of space
     * is available to submit new tasks to fsm. Returns true when available.
     * @param requiredCapacity
     * @return Returns true when available.
     */
    public boolean hasAvailableCapacity(final int requiredCapacity);

    /**
     * Called when loading snapshot.
     *
     * @param done callback
     */
    boolean onSnapshotLoad(final LoadSnapshotClosure done);

    /**
     * Called when saving snapshot synchronously, it MUST be called in state machine methods.
     * @param done
     */
    public void onSnapshotSaveSync(SaveSnapshotClosure done);

    /**
     * Called when saving snapshot.
     *
     * @param done callback
     */
    boolean onSnapshotSave(final SaveSnapshotClosure done);

    /**
     * Called when the leader stops.
     *
     * @param status status info
     */
    boolean onLeaderStop(final Status status);

    /**
     * Called when the leader starts.
     *
     * @param term current term
     */
    boolean onLeaderStart(final long term);

    /**
     * Called when start following a leader.
     *
     * @param ctx context of leader change
     */
    boolean onStartFollowing(final LeaderChangeContext ctx);

    /**
     * Called when stop following a leader.
     *
     * @param ctx context of leader change
     */
    boolean onStopFollowing(final LeaderChangeContext ctx);

    /**
     * Called when error happens.
     *
     * @param error error info
     */
    boolean onError(final RaftException error);

    /**
     * Returns the last log entry index to apply state machine.
     */
    long getLastAppliedIndex();

    /**
     * Returns the last log entry that was committed to raft group.
     */
    long getLastCommittedIndex();

    /**
     * Called after shutdown to wait it terminates.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;
}
