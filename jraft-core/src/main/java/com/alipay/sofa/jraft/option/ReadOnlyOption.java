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
package com.alipay.sofa.jraft.option;

import org.apache.commons.lang.StringUtils;

import com.alipay.sofa.jraft.entity.EnumOutter;

/**
 * Read only options.
 *
 * @author dennis
 */
public enum ReadOnlyOption {

    // ReadOnlySafe guarantees the linearizability of the read only request by
    // communicating with the quorum. It is the default and suggested option.
    ReadOnlySafe,
    // ReadOnlyLeaseBased ensures linearizability of the read only request by
    // relying on the leader lease. It can be affected by clock drift.
    // If the clock drift is unbounded, leader might keep the lease longer than it
    // should (clock can move backward/pause without any bound). ReadIndex is not safe
    // in that case.
    ReadOnlyLeaseBased;

    public static EnumOutter.ReadOnlyType convertMsgType(ReadOnlyOption option) {
        return ReadOnlyOption.ReadOnlyLeaseBased.equals(option) ? EnumOutter.ReadOnlyType.READ_ONLY_LEASE_BASED
            : EnumOutter.ReadOnlyType.READ_ONLY_SAFE;
    }

    public static ReadOnlyOption valueOfWithDefault(EnumOutter.ReadOnlyType readOnlyType, ReadOnlyOption defaultOption) {
        if (readOnlyType == null) {
            // for old version of messages
            return defaultOption;
        }
        return EnumOutter.ReadOnlyType.READ_ONLY_SAFE.equals(readOnlyType) ? ReadOnlyOption.ReadOnlySafe
            : ReadOnlyOption.ReadOnlyLeaseBased;
    }
}
