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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.error.LogEntryCorruptedException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolsFactory;
import com.alipay.sofa.jraft.util.Utils;

/**
 * The iterator implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 3:28:37 PM
 */
public class IteratorImpl {

    private final FSMCallerImpl fsmCaller;
    private final LogManager    logManager;
    private final List<Closure> closures;
    private final long          firstClosureIndex;
    private long                currentIndex;
    private final long          committedIndex;
    private long                fsmCommittedIndex;                // fsm commit index
    private LogEntry            currEntry        = new LogEntry(); // blank entry
    private final AtomicLong    applyingIndex;
    private RaftException       error;
    private boolean             autoCommitPerLog = false;         // Default enabled

    public IteratorImpl(final FSMCallerImpl fsmCaller, final LogManager logManager, final List<Closure> closures,
                        final long firstClosureIndex, final long lastAppliedIndex, final long committedIndex,
                        final AtomicLong applyingIndex) {
        super();
        this.fsmCaller = fsmCaller;
        this.fsmCommittedIndex = -1L;
        this.logManager = logManager;
        this.closures = closures;
        this.firstClosureIndex = firstClosureIndex;
        this.currentIndex = lastAppliedIndex;
        this.committedIndex = committedIndex;
        this.applyingIndex = applyingIndex;
        next();
    }

    @Override
    public String toString() {
        return "IteratorImpl [fsmCaller=" + fsmCaller + ", logManager=" + logManager + ", closures=" + closures
               + ", firstClosureIndex=" + firstClosureIndex + ", currentIndex=" + currentIndex + ", committedIndex="
               + committedIndex + ", fsmCommittedIndex=" + fsmCommittedIndex + ", currEntry=" + currEntry
               + ", applyingIndex=" + applyingIndex + ", error=" + error + "]";
    }

    public LogEntry entry() {
        return this.currEntry;
    }

    public RaftException getError() {
        return this.error;
    }

    public boolean isGood() {
        return this.currentIndex <= this.committedIndex && !hasError();
    }

    public boolean hasError() {
        return this.error != null;
    }

    public void setAutoCommitPerLog(boolean status) {
        this.autoCommitPerLog = status;
    }

    public boolean getAutoCommitPerLog() {
        return this.autoCommitPerLog;
    }

    /**
     * Move to next
     */
    public void next() {
        this.currEntry = null; //release current entry
        //get next entry
        if (this.currentIndex <= this.committedIndex) {
            ++this.currentIndex;
            if (this.currentIndex <= this.committedIndex) {
                try {
                    this.currEntry = this.logManager.getEntry(this.currentIndex);
                    if (this.currEntry == null) {
                        getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_LOG);
                        getOrCreateError().getStatus().setError(-1,
                            "Fail to get entry at index=%d while committed_index=%d", this.currentIndex,
                            this.committedIndex);
                    }
                } catch (final LogEntryCorruptedException e) {
                    getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_LOG);
                    getOrCreateError().getStatus().setError(RaftError.EINVAL, e.getMessage());
                }
                this.applyingIndex.set(this.currentIndex);
            }
        }
    }

    public long getIndex() {
        return this.currentIndex;
    }

    public Closure done() {
        if (this.currentIndex < this.firstClosureIndex) {
            return null;
        }
        return this.closures.get((int) (this.currentIndex - this.firstClosureIndex));
    }

    protected void runTheRestClosureWithError() {
        for (long i = Math.max(this.currentIndex, this.firstClosureIndex); i <= this.committedIndex; i++) {
            final Closure done = this.closures.get((int) (i - this.firstClosureIndex));
            if (done != null) {
                Requires.requireNonNull(this.error, "error");
                Requires.requireNonNull(this.error.getStatus(), "error.status");
                final Status status = this.error.getStatus();
                ThreadPoolsFactory.runClosureInThread(this.fsmCaller.getNode().getGroupId(), done, status);
            }
        }
    }

    public boolean commit() {
        if (isGood() && this.currEntry != null && this.currEntry.getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA) {
            fsmCommittedIndex = this.currentIndex;
            //Commit last applied.
            this.fsmCaller.setLastApplied(currentIndex, this.currEntry.getId().getTerm());
            return true;
        }
        return false;
    }

    public void commitAndSnapshotSync(Closure done) {
        if (commit()) {
            this.fsmCaller.getNode().snapshotSync(done);
        } else {
            Utils.runClosure(done, new Status(RaftError.ECANCELED, "Fail to commit, logIndex=" + currentIndex
                                                                   + ", committedIndex=" + committedIndex));
        }
    }

    public void setErrorAndRollback(final long ntail, final Status st) {
        Requires.requireTrue(ntail > 0, "Invalid ntail=" + ntail);
        if (this.currEntry == null || this.currEntry.getType() != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
            this.currentIndex -= ntail;
        } else {
            this.currentIndex -= ntail - 1;
        }
        if (fsmCommittedIndex >= 0) {
            // can't roll back before fsmCommittedIndex.
            this.currentIndex = Math.max(this.currentIndex, fsmCommittedIndex + 1);
        }
        this.currEntry = null;
        getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE);
        getOrCreateError().getStatus().setError(RaftError.ESTATEMACHINE,
            "StateMachine meet critical error when applying one or more tasks since index=%d, %s", this.currentIndex,
            st != null ? st.toString() : "none");

    }

    private RaftException getOrCreateError() {
        if (this.error == null) {
            this.error = new RaftException();
        }
        return this.error;
    }
}
