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
package com.alipay.sofa.jraft.rhea.client;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.option.CliOptions;

/**
 * RheaKV client command-line service.
 *
 * @author jiachun.fjc
 */
public interface RheaKVCliService extends Lifecycle<CliOptions> {

    /**
     * Send a split instruction to the specified region.
     *
     * @param regionId    region id
     * @param newRegionId id of the new region after splitting
     * @param groupId     the raft group id
     * @param conf        current configuration
     * @return operation status
     */
    Status rangeSplit(final long regionId, final long newRegionId, final String groupId, final Configuration conf);
}
