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
import java.util.Objects;

import com.alipay.sofa.jraft.util.Copiable;

/**
 *
 * @author jiachun.fjc
 */
public class RegionEpoch implements Copiable<RegionEpoch>, Comparable<RegionEpoch>, Serializable {

    private static final long serialVersionUID = -3752136007698056705L;

    // Conf change version, auto increment when add or remove peer
    private long              confVer;
    // Region version, auto increment when split or merge
    private long              version;

    public RegionEpoch() {
    }

    public RegionEpoch(long confVer, long version) {
        this.confVer = confVer;
        this.version = version;
    }

    public long getConfVer() {
        return confVer;
    }

    public void setConfVer(long confVer) {
        this.confVer = confVer;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public RegionEpoch copy() {
        return new RegionEpoch(this.confVer, this.version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RegionEpoch that = (RegionEpoch) o;
        return confVer == that.confVer && version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(confVer, version);
    }

    @Override
    public String toString() {
        return "RegionEpoch{" + "confVer=" + confVer + ", version=" + version + '}';
    }

    @Override
    public int compareTo(RegionEpoch o) {
        if (this.version == o.version) {
            return (int) (this.confVer - o.confVer);
        }
        return (int) (this.version - o.version);
    }
}
