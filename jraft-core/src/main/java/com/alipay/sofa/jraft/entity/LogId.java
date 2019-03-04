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

import com.alipay.sofa.jraft.util.Copiable;

/**
 * Log identifier.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:12:29 PM
 */
public class LogId implements Comparable<LogId>, Copiable<LogId>, Serializable {

    private static final long serialVersionUID = -6680425579347357313L;

    private long              index;
    private long              term;

    @Override
    public LogId copy() {
        return new LogId(index, term);
    }

    public LogId() {
        this(0, 0);
    }

    public LogId(long index, long term) {
        super();
        this.setIndex(index);
        this.setTerm(term);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (index ^ (index >>> 32));
        result = prime * result + (int) (term ^ (term >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LogId other = (LogId) obj;
        if (index != other.index) {
            return false;
        }
        if (term != other.term) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(LogId o) {
        // Compare term at first
        final int c = Long.compare(this.getTerm(), o.getTerm());
        if (c == 0) {
            return Long.compare(this.getIndex(), o.getIndex());
        } else {
            return c;
        }
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "LogId [index=" + this.index + ", term=" + this.term + "]";
    }

}
