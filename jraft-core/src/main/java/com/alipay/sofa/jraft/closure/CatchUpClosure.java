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
package com.alipay.sofa.jraft.closure;

import java.util.concurrent.ScheduledFuture;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

/**
 * A catchup closure for peer to catch up.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 2:15:05 PM
 */
public abstract class CatchUpClosure implements Closure {

    private long               maxMargin;
    private ScheduledFuture<?> timer;
    private boolean            hasTimer;
    private boolean            errorWasSet;

    private final Status       status = Status.OK();

    public Status getStatus() {
        return this.status;
    }

    public long getMaxMargin() {
        return this.maxMargin;
    }

    public void setMaxMargin(long maxMargin) {
        this.maxMargin = maxMargin;
    }

    public ScheduledFuture<?> getTimer() {
        return this.timer;
    }

    public void setTimer(ScheduledFuture<?> timer) {
        this.timer = timer;
        this.hasTimer = true;
    }

    public boolean hasTimer() {
        return this.hasTimer;
    }

    public boolean isErrorWasSet() {
        return this.errorWasSet;
    }

    public void setErrorWasSet(boolean errorWasSet) {
        this.errorWasSet = errorWasSet;
    }
}
