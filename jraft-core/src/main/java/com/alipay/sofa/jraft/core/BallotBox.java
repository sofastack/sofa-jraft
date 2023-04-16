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
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SegmentList;

/**
 * Ballot box for voting.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 2:32:10 PM
 */
@ThreadSafe
public class BallotBox implements Lifecycle<BallotBoxOptions>, Describer {

    private static final Logger       LOG                = LoggerFactory.getLogger(BallotBox.class);

    private FSMCaller                 waiter;
    private ClosureQueue              closureQueue;
    private final StampedLock         stampedLock        = new StampedLock();
    private long                      lastCommittedIndex = 0;
    private long                      pendingIndex;
    private final SegmentList<Ballot> pendingMetaQueue   = new SegmentList<>(false);
    private BallotBoxOptions          opts;

    @OnlyForTest
    long getPendingIndex() {
        return this.pendingIndex;
    }

    @OnlyForTest
    SegmentList<Ballot> getPendingMetaQueue() {
        return this.pendingMetaQueue;
    }

    public long getLastCommittedIndex() {
        long stamp = this.stampedLock.tryOptimisticRead();
        final long optimisticVal = this.lastCommittedIndex;
        if (this.stampedLock.validate(stamp)) {
            return optimisticVal;
        }
        stamp = this.stampedLock.readLock();
        try {
            return this.lastCommittedIndex;
        } finally {
            this.stampedLock.unlockRead(stamp);
        }
    }

    @Override
    public boolean init(final BallotBoxOptions opts) {
        if (opts.getWaiter() == null || opts.getClosureQueue() == null) {
            LOG.error("waiter or closure queue is null.");
            return false;
        }
        this.opts = opts;
        this.waiter = opts.getWaiter();
        this.closureQueue = opts.getClosureQueue();
        return true;
    }

    /**
     * Called by leader, otherwise the behavior is undefined
     * Set logs in [first_log_index, last_log_index] are stable at |peer|.
     */
    public boolean commitAt(final long firstLogIndex, final long lastLogIndex, final PeerId peer) {
        // TODO  use lock-free algorithm here?
        final long stamp = this.stampedLock.writeLock();
        long lastCommittedIndex = 0;
        try {
            if (this.pendingIndex == 0) {
                return false;
            }
            if (lastLogIndex < this.pendingIndex) {
                return true;
            }

            if (lastLogIndex >= this.pendingIndex + this.pendingMetaQueue.size()) {
                throw new ArrayIndexOutOfBoundsException();
            }

            final long startAt = Math.max(this.pendingIndex, firstLogIndex);
            Ballot.PosHint hint = new Ballot.PosHint();
            for (long logIndex = startAt; logIndex <= lastLogIndex; logIndex++) {
                final Ballot bl = this.pendingMetaQueue.get((int) (logIndex - this.pendingIndex));
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
            this.pendingMetaQueue.removeFromFirst((int) (lastCommittedIndex - this.pendingIndex) + 1);
            LOG.debug("Node {} committed log fromIndex={}, toIndex={}.", this.opts.getNodeId(), this.pendingIndex,
                lastCommittedIndex);
            this.pendingIndex = lastCommittedIndex + 1;
            this.lastCommittedIndex = lastCommittedIndex;
        } finally {
            this.stampedLock.unlockWrite(stamp);
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
        final long stamp = this.stampedLock.writeLock();
        try {
            this.pendingMetaQueue.clear();
            this.pendingIndex = 0;
            this.closureQueue.clear();
        } finally {
            this.stampedLock.unlockWrite(stamp);
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
    public boolean resetPendingIndex(final long newPendingIndex) {
        final long stamp = this.stampedLock.writeLock();
        try {
            if (!(this.pendingIndex == 0 && this.pendingMetaQueue.isEmpty())) {
                LOG.error("Node {} resetPendingIndex fail, pendingIndex={}, pendingMetaQueueSize={}.",
                    this.opts.getNodeId(), this.pendingIndex, this.pendingMetaQueue.size());
                return false;
            }
            if (newPendingIndex <= this.lastCommittedIndex) {
                LOG.error("Node {} resetPendingIndex fail, newPendingIndex={}, lastCommittedIndex={}.",
                    this.opts.getNodeId(), newPendingIndex, this.lastCommittedIndex);
                return false;
            }
            this.pendingIndex = newPendingIndex;
            this.closureQueue.resetFirstIndex(newPendingIndex);
            return true;
        } finally {
            this.stampedLock.unlockWrite(stamp);
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
    public boolean appendPendingTask(final Configuration conf, final Configuration oldConf, final Closure done) {
        final Ballot bl = new Ballot();
        if (!bl.init(conf, oldConf)) {
            LOG.error("Fail to init ballot.");
            return false;
        }
        final long stamp = this.stampedLock.writeLock();
        try {
            if (this.pendingIndex <= 0) {
                LOG.error("Node {} fail to appendingTask, pendingIndex={}.", this.opts.getNodeId(), this.pendingIndex);
                return false;
            }
            this.pendingMetaQueue.add(bl);
            this.closureQueue.appendPendingClosure(done);
            return true;
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
    }

    /**
     * Called by follower, otherwise the behavior is undefined.
     * Set committed index received from leader
     *
     * @param lastCommittedIndex last committed index
     * @return returns true if set success
     */
    public boolean setLastCommittedIndex(final long lastCommittedIndex) {
        boolean doUnlock = true;
        final long stamp = this.stampedLock.writeLock();
        try {
            if (this.pendingIndex != 0 || !this.pendingMetaQueue.isEmpty()) {
                Requires.requireTrue(lastCommittedIndex < this.pendingIndex,
                    "Node changes to leader, pendingIndex=%d, param lastCommittedIndex=%d", this.pendingIndex,
                    lastCommittedIndex);
                return false;
            }
            if (!this.waiter.hasAvailableCapacity(1)) {
                LOG.warn("Node {} fsm is busy, can't set lastCommittedIndex to be {}, lastCommittedIndex={}.",
                    this.opts.getNodeId(), lastCommittedIndex, this.lastCommittedIndex);
                return false;
            }
            if (lastCommittedIndex < this.lastCommittedIndex) {
                return false;
            }
            if (lastCommittedIndex > this.lastCommittedIndex) {
                this.lastCommittedIndex = lastCommittedIndex;
                this.stampedLock.unlockWrite(stamp);
                doUnlock = false;
                this.waiter.onCommitted(lastCommittedIndex);
            }
        } finally {
            if (doUnlock) {
                this.stampedLock.unlockWrite(stamp);
            }
        }
        return true;
    }

    @Override
    public void shutdown() {
        clearPendingTasks();
    }

    @Override
    public void describe(final Printer out) {
        long _lastCommittedIndex;
        long _pendingIndex;
        long _pendingMetaQueueSize;
        long stamp = this.stampedLock.tryOptimisticRead();
        if (this.stampedLock.validate(stamp)) {
            _lastCommittedIndex = this.lastCommittedIndex;
            _pendingIndex = this.pendingIndex;
            _pendingMetaQueueSize = this.pendingMetaQueue.size();
        } else {
            stamp = this.stampedLock.readLock();
            try {
                _lastCommittedIndex = this.lastCommittedIndex;
                _pendingIndex = this.pendingIndex;
                _pendingMetaQueueSize = this.pendingMetaQueue.size();
            } finally {
                this.stampedLock.unlockRead(stamp);
            }
        }
        out.print("  lastCommittedIndex: ") //
            .println(_lastCommittedIndex);
        out.print("  pendingIndex: ") //
            .println(_pendingIndex);
        out.print("  pendingMetaQueueSize: ") //
            .println(_pendingMetaQueueSize);
    }
}
