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
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * The iterator implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 3:28:37 PM
 */
public class IteratorImpl {

    private final StateMachine  fsm;
    private final LogManager    logManager;
    private final List<Closure> closures;
    private final long          firstClosureIndex;
    private long                currentIndex;
    private final long          committedIndex;
    private LogEntry            currEntry = new LogEntry(); // blank entry
    private final AtomicLong    applyingIndex;
    private RaftException       error;

    public IteratorImpl(StateMachine fsm, LogManager logManager, List<Closure> closures, long firstClosureIndex,
                        long lastAppliedIndex, long committedIndex, AtomicLong applyingIndex) {
        super();
        this.fsm = fsm;
        this.logManager = logManager;
        this.closures = closures;
        this.firstClosureIndex = firstClosureIndex;
        this.currentIndex = lastAppliedIndex;
        this.committedIndex = committedIndex;
        this.applyingIndex = applyingIndex;
        this.next();
    }

    @Override
    public String toString() {
        return "IteratorImpl [fsm=" + this.fsm + ", logManager=" + this.logManager + ", closures=" + this.closures
               + ", firstClosureIndex=" + this.firstClosureIndex + ", currentIndex=" + this.currentIndex
               + ", committedIndex=" + this.committedIndex + ", currEntry=" + this.currEntry + ", applyingIndex="
               + this.applyingIndex + ", error=" + this.error + "]";
    }

    public LogEntry entry() {
        return this.currEntry;
    }

    public RaftException getError() {
        return this.error;
    }

    public boolean isGood() {
        return currentIndex <= committedIndex && !hasError();
    }

    public boolean hasError() {
        return this.error != null;
    }

    /**
     * Move to next
     */
    public void next() {
        this.currEntry = null; //release current entry
        //get next entry
        if (currentIndex <= this.committedIndex) {
            ++currentIndex;
            if (currentIndex <= committedIndex) {
                currEntry = logManager.getEntry(currentIndex);
                if (currEntry == null) {
                    this.getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_LOG);
                    this.getOrCreateError()
                        .getStatus()
                        .setError(-1, "Fail to get entry at index=%d while committed_index=%d", currentIndex,
                            committedIndex);
                }
                this.applyingIndex.set(currentIndex);
            }
        }
    }

    public long getIndex() {
        return this.currentIndex;
    }

    public Closure done() {
        if (currentIndex < firstClosureIndex) {
            return null;
        }
        return this.closures.get((int) (currentIndex - firstClosureIndex));
    }

    protected void runTheRestClosureWithError() {
        for (long i = Math.max(this.currentIndex, this.firstClosureIndex); i <= this.committedIndex; i++) {
            final Closure done = this.closures.get((int) (i - firstClosureIndex));
            if (done != null) {
                Requires.requireNonNull(this.error, "error");
                Requires.requireNonNull(this.error.getStatus(), "error.status");
                final Status status = this.error.getStatus();
                Utils.runClosureInThread(done, status);
            }
        }
    }

    public void setErrorAndRollback(long ntail, Status st) {
        Requires.requireTrue(ntail > 0, "Invalid ntail=" + ntail);
        if (currEntry == null || currEntry.getType() != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
            currentIndex -= ntail;
        } else {
            currentIndex -= ntail - 1;
        }
        currEntry = null;
        getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE);
        getOrCreateError().getStatus().setError(RaftError.ESTATEMACHINE,
            "StateMachine meet critical error when applying one  or more tasks since index=%d, %s", currentIndex,
            st != null ? st.toString() : "none");

    }

    private RaftException getOrCreateError() {
        if (error == null) {
            error = new RaftException();
        }
        return error;
    }
}
