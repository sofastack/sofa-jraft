/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.alipay.sofa.jraft.util.timer.wheel;

public abstract class TimerTask implements Runnable {

    /**
     * timestamp in millisecond
     */
    private final long     delayMs;

    private TimerTaskEntry timerTaskEntry;

    public TimerTask(long delayMs) {
        this.delayMs = delayMs;
    }

    public void cancel() {
        synchronized (this) {
            if (this.timerTaskEntry != null) {
                this.timerTaskEntry.remove();
            }
            this.timerTaskEntry = null;
        }
    }

    public void setTimerTaskEntry(final TimerTaskEntry entry) {
        synchronized (this) {
            // if this timerTask is already held by an existing timer task entry,
            // we will remove such an entry first.
            if (this.timerTaskEntry != null && this.timerTaskEntry != entry) {
                this.timerTaskEntry.remove();
            }
            this.timerTaskEntry = entry;
        }
    }

    public long getDelayMs() {
        return this.delayMs;
    }

    public TimerTaskEntry getTimerTaskEntry() {
        return this.timerTaskEntry;
    }
}
