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
package com.alipay.sofa.jraft.rhea.metadata;

import java.io.Serializable;

import com.alipay.sofa.jraft.util.Copiable;

/**
 *
 * @author jiachun.fjc
 */
public class TimeInterval implements Copiable<TimeInterval>, Serializable {

    private static final long serialVersionUID = 2454987958697466235L;

    // The unix timestamp in milliseconds of the start of this period.
    private long              startTimestamp;
    // The unix timestamp in seconds of the end of this period.
    private long              endTimestamp;

    public TimeInterval() {
    }

    public TimeInterval(long startTimestamp, long endTimestamp) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    @Override
    public TimeInterval copy() {
        return new TimeInterval(this.startTimestamp, this.endTimestamp);
    }

    @Override
    public String toString() {
        return "TimeInterval{" + "startTimestamp=" + startTimestamp + ", endTimestamp=" + endTimestamp + '}';
    }
}
