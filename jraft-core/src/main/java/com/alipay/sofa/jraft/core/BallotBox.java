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
package com.alipay.sofa.jraft.core;

import java.util.concurrent.locks.StampedLock;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.Ballot;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.BallotBoxOptions;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Ballot box for voting.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 2:32:10 PM
 */
@ThreadSafe
public class BallotBox implements Lifecycle<BallotBoxOptions> {

    private static final Logger      LOG                = LoggerFactory.getLogger(BallotBox.class);

    private FSMCaller                waiter;
    private ClosureQueue             closureQueue;
    private final StampedLock        stampedLock        = new StampedLock();
    private long                     lastCommittedIndex = 0;
    private long                     pendingIndex;
    private final ArrayDeque<Ballot> pendingMetaQueue   = new ArrayDeque<>();

    @OnlyForTest
    long getPendingIndex() {
        return this.pendingIndex;
    }

    @OnlyForTest
    ArrayDeque<Ballot> getPendingMetaQueue() {
        return this.pendingMetaQueue;
    }

    public long getLastCommittedIndex() {
        long stamp = stampedLock.tryOptimisticRead();
        long optimisticVal = this.lastCommittedIndex;
        if (stampedLock.validate(stamp)) {
            return optimisticVal;
        }
        stamp = stampedLock.readLock();
        try {
            return this.lastCommittedIndex;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    @Override
    public boolean init(BallotBoxOptions opts) {
        if (opts.getWaiter() == null || opts.getClosureQueue() == null) {
            LOG.error("waiter or closure queue is null");
            return false;
        }
        this.waiter = opts.getWaiter();
        this.closureQueue = opts.getClosureQueue();
        return true;
    }

    /**
     * Called by leader, otherwise the behavior is undefined
     * Set logs in [first_log_index, last_log_index] are stable at |peer|.
     */
    public boolean commitAt(long firstLogIndex, long lastLogIndex, PeerId peer) {
        //TODO  use lock-free algorithm here?
        final long stamp = stampedLock.writeLock();
        long lastCommittedIndex = 0;
        try {
            if (pendingIndex == 0) {
                return false;
            }
            if (lastLogIndex < pendingIndex) {
                return true;
            }

            if (lastLogIndex >= pendingIndex + pendingMetaQueue.size()) {
                throw new ArrayIndexOutOfBoundsException();
            }

            final long startAt = Math.max(pendingIndex, firstLogIndex);
            Ballot.PosHint hint = new Ballot.PosHint();
            for (long logIndex = startAt; logIndex <= lastLogIndex; logIndex++) {
                final Ballot bl = this.pendingMetaQueue.get((int) (logIndex - pendingIndex));
                hint = bl.grant(peer, hint);
                if (bl.isGranted()) {
                    lastCommittedIndex = logIndex;
                }
            }
            if (lastCommittedIndex == 0) {
                return true;
            }
            // When removing a peer off the raft group which contains even number of
            // peers, the quorum would decrease by 1, e.g. 3 of 4 changes to 2 of 3. In
            // this case, the log after removal may be committed before some previous
            // logs, since we use the new configuration to deal the quorum of the
            // removal request, we think it's safe to commit all the uncommitted
            // previous logs, which is not well proved right now
            pendingMetaQueue.removeRange(0, (int) (lastCommittedIndex - pendingIndex) + 1);
            LOG.debug("Committed log fromIndex={}, toIndex={}.", pendingIndex, lastCommittedIndex);
            pendingIndex = lastCommittedIndex + 1;
            this.lastCommittedIndex = lastCommittedIndex;
        } finally {
            stampedLock.unlockWrite(stamp);
        }
        this.waiter.onCommitted(lastCommittedIndex);
        return true;
    }

    /**
     * Called when the leader steps down, otherwise the behavior is undefined
     * When a leader steps down, the uncommitted user applications should
     * fail immediately, which the new leader will deal whether to commit or
     * truncate.
     */
    public void clearPendingTasks() {
        final long stamp = stampedLock.writeLock();
        try {
            this.pendingMetaQueue.clear();
            this.pendingIndex = 0;
            this.closureQueue.clear();
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    /**
     * Called when a candidate becomes the new leader, otherwise the behavior is
     * undefined.
     * According the the raft algorithm, the logs from previous terms can't be
     * committed until a log at the new term becomes committed, so
     * |newPendingIndex| should be |last_log_index| + 1.
     * @param newPendingIndex pending index of new leader
     * @return returns true if reset success
     */
    public boolean resetPendingIndex(long newPendingIndex) {
        final long stamp = stampedLock.writeLock();
        try {
            if (!(pendingIndex == 0 && pendingMetaQueue.isEmpty())) {
                LOG.error("resetPendingIndex fail, pendingIndex={}, pendingMetaQueueSize={}", pendingIndex,
                    pendingMetaQueue.size());
                return false;
            }
            if (newPendingIndex <= this.lastCommittedIndex) {
                LOG.error("resetPendingIndex fail, newPendingIndex={}, lastCommittedIndex={}", newPendingIndex,
                    lastCommittedIndex);
                return false;
            }
            this.pendingIndex = newPendingIndex;
            this.closureQueue.resetFirstIndex(newPendingIndex);
            return true;
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    /**
     * Called by leader, otherwise the behavior is undefined
     * Store application context before replication.
     *
     * @param conf      current configuration
     * @param oldConf   old configuration
     * @param done      callback
     * @return          returns true on success
     */
    public boolean appendPendingTask(Configuration conf, Configuration oldConf, Closure done) {
        final Ballot bl = new Ballot();
        if (!bl.init(conf, oldConf)) {
            LOG.error("Fail to init ballot");
            return false;
        }
        final long stamp = stampedLock.writeLock();
        try {
            if (pendingIndex <= 0) {
                LOG.error("Fail to appendingTask, pendingIndex={}", pendingIndex);
                return false;
            }
            this.pendingMetaQueue.add(bl);
            this.closureQueue.appendPendingClosure(done);
            return true;
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    /**
     * Called by follower, otherwise the behavior is undefined.
     *  Set committed index received from leader
     * @param lastCommittedIndex last committed index
     * @return returns true if set success
     */
    public boolean setLastCommittedIndex(long lastCommittedIndex) {
        boolean doUnlock = true;
        final long stamp = stampedLock.writeLock();
        try {
            if (pendingIndex != 0 || !pendingMetaQueue.isEmpty()) {
                Requires.requireTrue(lastCommittedIndex < this.pendingIndex,
                    "Node changes to leader, pendingIndex=%d, param lastCommittedIndex=%d", pendingIndex,
                    lastCommittedIndex);
                return false;
            }
            if (lastCommittedIndex < this.lastCommittedIndex) {
                return false;
            }
            if (lastCommittedIndex > this.lastCommittedIndex) {
                this.lastCommittedIndex = lastCommittedIndex;
                stampedLock.unlockWrite(stamp);
                doUnlock = false;
                this.waiter.onCommitted(lastCommittedIndex);
            }
        } finally {
            if (doUnlock) {
                stampedLock.unlockWrite(stamp);
            }
        }
        return true;
    }

    @Override
    public void shutdown() {
        this.clearPendingTasks();
    }
}
